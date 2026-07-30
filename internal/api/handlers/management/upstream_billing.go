package management

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

const (
	defaultUpstreamBillingProbeIntervalMinutes = 30
	upstreamBillingProbeTimeout                = 10 * time.Second
	upstreamBillingProbeMaxBodyBytes           = 64 * 1024
)

type upstreamBillingProbeSettings struct {
	IntervalMinutes int `json:"interval-minutes"`
}

type upstreamBillingProbeEntry struct {
	AuthIndex     string    `json:"auth-index"`
	Provider      string    `json:"provider"`
	ProviderName  string    `json:"provider-name"`
	BaseURL       string    `json:"base-url"`
	APIKeyPreview string    `json:"api-key-preview,omitempty"`
	Status        string    `json:"status"`
	Error         string    `json:"error,omitempty"`
	ObservedAt    time.Time `json:"observed-at,omitempty"`

	GroupRateMultiplier     *float64 `json:"group-rate-multiplier,omitempty"`
	UserRateMultiplier      *float64 `json:"user-rate-multiplier,omitempty"`
	ResolvedRateMultiplier  *float64 `json:"resolved-rate-multiplier,omitempty"`
	PeakRateEnabled         *bool    `json:"peak-rate-enabled,omitempty"`
	PeakRateMultiplier      *float64 `json:"peak-rate-multiplier,omitempty"`
	AppliedPeakMultiplier   *float64 `json:"applied-peak-multiplier,omitempty"`
	EffectiveRateMultiplier *float64 `json:"effective-rate-multiplier,omitempty"`
	PeakStart               string   `json:"peak-start,omitempty"`
	PeakEnd                 string   `json:"peak-end,omitempty"`
	Timezone                string   `json:"timezone,omitempty"`
}

type upstreamBillingProbeCache struct {
	updatedAt time.Time
	entries   []upstreamBillingProbeEntry
}

type upstreamBillingProbePayload struct {
	Settings  upstreamBillingProbeSettings `json:"settings"`
	UpdatedAt *time.Time                   `json:"updated-at,omitempty"`
	Items     []upstreamBillingProbeEntry  `json:"items"`
}

type upstreamBillingProbeResponse struct {
	Object                  string   `json:"object"`
	SchemaVersion           int      `json:"schema_version"`
	BillingScope            string   `json:"billing_scope"`
	GroupRateMultiplier     *float64 `json:"group_rate_multiplier"`
	UserRateMultiplier      *float64 `json:"user_rate_multiplier"`
	ResolvedRateMultiplier  *float64 `json:"resolved_rate_multiplier"`
	PeakRateEnabled         *bool    `json:"peak_rate_enabled"`
	PeakStart               *string  `json:"peak_start"`
	PeakEnd                 *string  `json:"peak_end"`
	PeakRateMultiplier      *float64 `json:"peak_rate_multiplier"`
	AppliedPeakMultiplier   *float64 `json:"applied_peak_multiplier"`
	EffectiveRateMultiplier *float64 `json:"effective_rate_multiplier"`
	Timezone                *string  `json:"timezone"`
	ObservedAt              string   `json:"observed_at"`
}

func (h *Handler) ensureUpstreamBillingProbeDefaultsLocked() {
	if h == nil || h.cfg == nil {
		return
	}
	if h.cfg.UpstreamBillingProbe.IntervalMinutes <= 0 {
		h.cfg.UpstreamBillingProbe.IntervalMinutes = defaultUpstreamBillingProbeIntervalMinutes
	}
}

func (h *Handler) upstreamBillingProbeIntervalMinutes() int {
	if h == nil {
		return defaultUpstreamBillingProbeIntervalMinutes
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ensureUpstreamBillingProbeDefaultsLocked()
	return h.cfg.UpstreamBillingProbe.IntervalMinutes
}

func (h *Handler) upstreamBillingProbeSettingsPayload() upstreamBillingProbeSettings {
	return upstreamBillingProbeSettings{IntervalMinutes: h.upstreamBillingProbeIntervalMinutes()}
}

func isOpenAICompatibilityAuth(auth *coreauth.Auth) bool {
	if auth == nil {
		return false
	}
	provider := strings.ToLower(strings.TrimSpace(auth.Provider))
	if provider == "openai-compatibility" || strings.HasPrefix(provider, "openai-compatible-") || strings.HasPrefix(provider, "openai-compatibility:") {
		return true
	}
	return strings.TrimSpace(auth.Attributes["compat_name"]) != "" || strings.TrimSpace(auth.Attributes["provider_key"]) != ""
}

func (h *Handler) startUpstreamBillingProbeLoop() {
	go func() {
		// ponytail: one background loop is enough; add lifecycle stop only if handlers become short-lived.
		for {
			h.refreshUpstreamBilling(context.Background())
			interval := h.upstreamBillingProbeIntervalMinutes()
			if interval <= 0 {
				interval = defaultUpstreamBillingProbeIntervalMinutes
			}
			time.Sleep(time.Duration(interval) * time.Minute)
		}
	}()
}

func (h *Handler) upstreamBillingProbeCacheSnapshot() []upstreamBillingProbeEntry {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		return nil
	}

	auths := manager.List()
	if len(auths) == 0 {
		return nil
	}

	out := make([]upstreamBillingProbeEntry, 0, len(auths))
	for _, auth := range auths {
		entry, ok := h.upstreamBillingProbeEntryForAuth(auth)
		if !ok {
			continue
		}
		out = append(out, entry)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Provider != out[j].Provider {
			return out[i].Provider < out[j].Provider
		}
		return out[i].AuthIndex < out[j].AuthIndex
	})
	return out
}

func (h *Handler) upstreamBillingProbeEntryForAuth(auth *coreauth.Auth) (upstreamBillingProbeEntry, bool) {
	if h == nil || auth == nil {
		return upstreamBillingProbeEntry{}, false
	}
	if !isOpenAICompatibilityAuth(auth) {
		return upstreamBillingProbeEntry{}, false
	}
	if auth.Disabled || auth.Status == coreauth.StatusDisabled {
		return upstreamBillingProbeEntry{}, false
	}
	key := strings.TrimSpace(auth.Attributes["api_key"])
	baseURL := strings.TrimSpace(auth.Attributes["base_url"])
	if key == "" || baseURL == "" {
		return upstreamBillingProbeEntry{}, false
	}
	authIndex := strings.TrimSpace(auth.EnsureIndex())
	if authIndex == "" {
		return upstreamBillingProbeEntry{}, false
	}
	providerName := strings.TrimSpace(auth.Label)
	if providerName == "" {
		providerName = strings.TrimSpace(auth.Attributes["compat_name"])
	}
	if providerName == "" {
		providerName = "openai-compatibility"
	}
	entry := upstreamBillingProbeEntry{
		AuthIndex:     authIndex,
		Provider:      strings.ToLower(strings.TrimSpace(auth.Provider)),
		ProviderName:  providerName,
		BaseURL:       baseURL,
		APIKeyPreview: util.HideAPIKey(key),
		Status:        "skipped",
		ObservedAt:    time.Time{},
	}
	if h != nil {
		if cache := h.loadUpstreamBillingProbeCache(); cache != nil {
			for _, cached := range cache.entries {
				if cached.AuthIndex != authIndex {
					continue
				}
				return cached, true
			}
		}
	}
	return entry, true
}

func (h *Handler) loadUpstreamBillingProbeCache() *upstreamBillingProbeCache {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.upstreamBillingProbeCache == nil {
		return nil
	}
	copyCache := *h.upstreamBillingProbeCache
	if len(copyCache.entries) > 0 {
		copyCache.entries = append([]upstreamBillingProbeEntry(nil), copyCache.entries...)
	}
	return &copyCache
}

func (h *Handler) upstreamBillingProbeByAuthIndex() map[string]upstreamBillingProbeEntry {
	cache := h.loadUpstreamBillingProbeCache()
	if cache == nil || len(cache.entries) == 0 {
		return nil
	}
	out := make(map[string]upstreamBillingProbeEntry, len(cache.entries))
	for _, entry := range cache.entries {
		key := strings.TrimSpace(entry.AuthIndex)
		if key != "" {
			out[key] = entry
		}
	}
	return out
}

func (h *Handler) storeUpstreamBillingProbeCache(entries []upstreamBillingProbeEntry) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.upstreamBillingProbeCache == nil {
		h.upstreamBillingProbeCache = &upstreamBillingProbeCache{}
	}
	h.upstreamBillingProbeCache.updatedAt = time.Now().UTC()
	h.upstreamBillingProbeCache.entries = append([]upstreamBillingProbeEntry(nil), entries...)
}

func (h *Handler) GetUpstreamBillingProbe(c *gin.Context) {
	if h == nil || h.cfg == nil {
		c.JSON(http.StatusOK, upstreamBillingProbePayload{
			Settings: upstreamBillingProbeSettings{IntervalMinutes: defaultUpstreamBillingProbeIntervalMinutes},
			Items:    []upstreamBillingProbeEntry{},
		})
		return
	}
	var updatedAt *time.Time
	if cache := h.loadUpstreamBillingProbeCache(); cache != nil && !cache.updatedAt.IsZero() {
		updated := cache.updatedAt
		updatedAt = &updated
	}
	c.JSON(http.StatusOK, upstreamBillingProbePayload{
		Settings:  h.upstreamBillingProbeSettingsPayload(),
		UpdatedAt: updatedAt,
		Items:     h.upstreamBillingProbeCacheSnapshot(),
	})
}

func (h *Handler) PutUpstreamBillingProbe(c *gin.Context) {
	var body struct {
		IntervalMinutes *int `json:"interval-minutes"`
		Value           *struct {
			IntervalMinutes *int `json:"interval-minutes"`
		} `json:"value"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	interval := h.upstreamBillingProbeIntervalMinutes()
	if body.IntervalMinutes != nil {
		interval = *body.IntervalMinutes
	}
	if body.Value != nil && body.Value.IntervalMinutes != nil {
		interval = *body.Value.IntervalMinutes
	}
	if interval <= 0 {
		interval = defaultUpstreamBillingProbeIntervalMinutes
	}
	h.mu.Lock()
	if h.cfg != nil {
		h.cfg.UpstreamBillingProbe.IntervalMinutes = interval
	}
	ok := h.persistLocked(c)
	h.mu.Unlock()
	if ok {
		go h.refreshUpstreamBilling(context.Background())
	}
}

func (h *Handler) RefreshUpstreamBilling(c *gin.Context) {
	ctx := context.Background()
	if c != nil && c.Request != nil && c.Request.Context() != nil {
		ctx = c.Request.Context()
	}
	entries := h.refreshUpstreamBilling(ctx)
	c.JSON(http.StatusOK, gin.H{"updated-at": time.Now().UTC(), "items": entries})
}

func (h *Handler) refreshUpstreamBilling(ctx context.Context) []upstreamBillingProbeEntry {
	if h == nil || h.authManager == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, upstreamBillingProbeTimeout)
	defer cancel()

	auths := h.authManager.List()
	entries := make([]upstreamBillingProbeEntry, 0, len(auths))
	for _, auth := range auths {
		entry, ok := h.probeUpstreamBillingEntry(timeoutCtx, auth)
		if !ok {
			continue
		}
		entries = append(entries, entry)
	}
	h.storeUpstreamBillingProbeCache(entries)
	return entries
}

func (h *Handler) probeUpstreamBillingEntry(ctx context.Context, auth *coreauth.Auth) (upstreamBillingProbeEntry, bool) {
	if h == nil || auth == nil {
		return upstreamBillingProbeEntry{}, false
	}
	if !isOpenAICompatibilityAuth(auth) {
		return upstreamBillingProbeEntry{}, false
	}
	if auth.Disabled || auth.Status == coreauth.StatusDisabled {
		return upstreamBillingProbeEntry{}, false
	}
	key := strings.TrimSpace(auth.Attributes["api_key"])
	baseURL := strings.TrimSpace(auth.Attributes["base_url"])
	if key == "" || baseURL == "" {
		return upstreamBillingProbeEntry{}, false
	}
	authIndex := strings.TrimSpace(auth.EnsureIndex())
	if authIndex == "" {
		return upstreamBillingProbeEntry{}, false
	}
	providerName := strings.TrimSpace(auth.Label)
	if providerName == "" {
		providerName = strings.TrimSpace(auth.Attributes["compat_name"])
	}
	if providerName == "" {
		providerName = "openai-compatibility"
	}
	entry := upstreamBillingProbeEntry{
		AuthIndex:     authIndex,
		Provider:      strings.ToLower(strings.TrimSpace(auth.Provider)),
		ProviderName:  providerName,
		BaseURL:       baseURL,
		APIKeyPreview: util.HideAPIKey(key),
	}

	reqURL := strings.TrimRight(baseURL, "/") + "/v1/sub2api/billing"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		entry.Status = "failed"
		entry.Error = "request-build-failed"
		return entry, true
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	client := &http.Client{Timeout: upstreamBillingProbeTimeout}
	if auth.ProxyURL != "" {
		if proxyURL, errParse := url.Parse(auth.ProxyURL); errParse == nil && proxyURL != nil {
			client.Transport = &http.Transport{Proxy: http.ProxyURL(proxyURL)}
		}
	}
	resp, errDo := client.Do(req)
	if errDo != nil {
		entry.Status = "failed"
		entry.Error = "request-failed"
		return entry, true
	}
	defer func() { _ = resp.Body.Close() }()
	body, errRead := io.ReadAll(io.LimitReader(resp.Body, upstreamBillingProbeMaxBodyBytes+1))
	if errRead != nil {
		entry.Status = "failed"
		entry.Error = "response-read-failed"
		return entry, true
	}
	if len(body) > upstreamBillingProbeMaxBodyBytes {
		entry.Status = "failed"
		entry.Error = "response-too-large"
		return entry, true
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		entry.Status = "failed"
		entry.Error = fmt.Sprintf("http-%d", resp.StatusCode)
		return entry, true
	}
	data, errParse := parseUpstreamBillingProbeResponse(body)
	if errParse != nil {
		entry.Status = "failed"
		entry.Error = errParse.Error()
		return entry, true
	}
	entry.Status = "ok"
	entry.ObservedAt = data.ObservedAt
	if entry.ObservedAt.IsZero() {
		entry.ObservedAt = time.Now().UTC()
	}
	entry.GroupRateMultiplier = data.GroupRateMultiplier
	entry.UserRateMultiplier = data.UserRateMultiplier
	entry.ResolvedRateMultiplier = data.ResolvedRateMultiplier
	entry.PeakRateEnabled = data.PeakRateEnabled
	entry.PeakRateMultiplier = data.PeakRateMultiplier
	entry.AppliedPeakMultiplier = data.AppliedPeakMultiplier
	entry.EffectiveRateMultiplier = data.EffectiveRateMultiplier
	entry.PeakStart = data.PeakStart
	entry.PeakEnd = data.PeakEnd
	entry.Timezone = data.Timezone
	return entry, true
}

func parseUpstreamBillingProbeResponse(body []byte) (*upstreamBillingProbeEntry, error) {
	var response upstreamBillingProbeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	if response.Object != "sub2api.key_billing" || response.SchemaVersion != 1 || response.BillingScope != "token" {
		return nil, fmt.Errorf("unexpected billing schema")
	}
	if response.GroupRateMultiplier == nil || response.ResolvedRateMultiplier == nil || response.PeakRateEnabled == nil || response.EffectiveRateMultiplier == nil {
		return nil, fmt.Errorf("incomplete billing response")
	}
	for _, value := range []float64{*response.GroupRateMultiplier, *response.ResolvedRateMultiplier, *response.EffectiveRateMultiplier} {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("invalid billing multiplier")
		}
	}
	entry := &upstreamBillingProbeEntry{
		Status:                  "ok",
		GroupRateMultiplier:     response.GroupRateMultiplier,
		UserRateMultiplier:      response.UserRateMultiplier,
		ResolvedRateMultiplier:  response.ResolvedRateMultiplier,
		PeakRateEnabled:         response.PeakRateEnabled,
		PeakRateMultiplier:      response.PeakRateMultiplier,
		AppliedPeakMultiplier:   response.AppliedPeakMultiplier,
		EffectiveRateMultiplier: response.EffectiveRateMultiplier,
	}
	if response.PeakStart != nil {
		entry.PeakStart = strings.TrimSpace(*response.PeakStart)
	}
	if response.PeakEnd != nil {
		entry.PeakEnd = strings.TrimSpace(*response.PeakEnd)
	}
	if response.Timezone != nil {
		entry.Timezone = strings.TrimSpace(*response.Timezone)
	}
	if response.ObservedAt != "" {
		if ts, err := time.Parse(time.RFC3339Nano, response.ObservedAt); err == nil {
			entry.ObservedAt = ts.UTC()
		}
	}
	return entry, nil
}

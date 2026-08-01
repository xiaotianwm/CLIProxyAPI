package management

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

const (
	defaultUpstreamBillingProbeIntervalMinutes = 30
	defaultUpstreamHealthProbeModel            = "gpt-5.5"
	upstreamBillingProbeTimeout                = 5 * time.Second
	upstreamHealthProbeHistoryLimit            = 30
	upstreamBillingProbeMaxBodyBytes           = 64 * 1024
	upstreamHealthFastLatencyMS                = 10_000
	upstreamHealthExcellentPriority            = 100
	upstreamHealthNormalPriority               = 50
	upstreamHealthFailedPriority               = 1
	upstreamHealthProbeInstructions            = "Answer the arithmetic health check. Return only the integer."
	upstreamHealthProbeOperandMax              = 50
	upstreamHealthProbeMaxTokens               = 8
)

type upstreamProbeKind string

const (
	upstreamProbeBilling upstreamProbeKind = "billing"
	upstreamProbeHealth  upstreamProbeKind = "health"
)

type upstreamProbeTaskKey struct {
	AuthIndex string
	Kind      upstreamProbeKind
}

type upstreamProbeTask struct {
	Token       uint64
	Fingerprint [32]byte
	StartedAt   time.Time
	Cancel      context.CancelFunc
}

type upstreamProbeWork struct {
	Key         upstreamProbeTaskKey
	Auth        *coreauth.Auth
	HealthModel string
	Fingerprint [32]byte
}

type upstreamProbeTaskRef struct {
	AuthIndex string            `json:"auth-index"`
	Kind      upstreamProbeKind `json:"kind"`
}

type upstreamProbeSkippedTask struct {
	upstreamProbeTaskRef
	Reason string `json:"reason"`
}

type upstreamProbeDispatchResult struct {
	Accepted []upstreamProbeTaskRef     `json:"accepted"`
	Skipped  []upstreamProbeSkippedTask `json:"skipped"`
	Running  []upstreamProbeTaskRef     `json:"running"`
}

type upstreamHealthProbeChallenge struct {
	Prompt   string
	Expected string
	CacheKey string
}

type upstreamBillingProbeSettings struct {
	IntervalMinutes     int    `json:"interval-minutes"`
	HealthEnabled       bool   `json:"health-enabled"`
	HealthModel         string `json:"health-model"`
	AutoPriorityEnabled bool   `json:"auto-priority-enabled"`
}

type upstreamHealthProbeSample struct {
	Status       string    `json:"status"`
	LatencyMS    int64     `json:"latency-ms,omitempty"`
	HTTPStatus   int       `json:"http-status,omitempty"`
	Model        string    `json:"model,omitempty"`
	CachedTokens int64     `json:"cached-tokens,omitempty"`
	CheckedAt    time.Time `json:"checked-at"`
	Error        string    `json:"error,omitempty"`
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

	GroupRateMultiplier     *float64                    `json:"group-rate-multiplier,omitempty"`
	UserRateMultiplier      *float64                    `json:"user-rate-multiplier,omitempty"`
	ResolvedRateMultiplier  *float64                    `json:"resolved-rate-multiplier,omitempty"`
	PeakRateEnabled         *bool                       `json:"peak-rate-enabled,omitempty"`
	PeakRateMultiplier      *float64                    `json:"peak-rate-multiplier,omitempty"`
	AppliedPeakMultiplier   *float64                    `json:"applied-peak-multiplier,omitempty"`
	EffectiveRateMultiplier *float64                    `json:"effective-rate-multiplier,omitempty"`
	PeakStart               string                      `json:"peak-start,omitempty"`
	PeakEnd                 string                      `json:"peak-end,omitempty"`
	Timezone                string                      `json:"timezone,omitempty"`
	HealthHistory           []upstreamHealthProbeSample `json:"health-history,omitempty"`
}

type upstreamBillingProbeCache struct {
	updatedAt time.Time
	entries   []upstreamBillingProbeEntry
}

type upstreamBillingProbePayload struct {
	Settings  upstreamBillingProbeSettings `json:"settings"`
	UpdatedAt *time.Time                   `json:"updated-at,omitempty"`
	Items     []upstreamBillingProbeEntry  `json:"items"`
	Running   []upstreamProbeTaskRef       `json:"running"`
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
	h.cfg.UpstreamBillingProbe.HealthModel = strings.TrimSpace(h.cfg.UpstreamBillingProbe.HealthModel)
	if h.cfg.UpstreamBillingProbe.HealthModel == "" {
		h.cfg.UpstreamBillingProbe.HealthModel = defaultUpstreamHealthProbeModel
	}
}

func (h *Handler) upstreamBillingProbeIntervalMinutes() int {
	if h == nil {
		return defaultUpstreamBillingProbeIntervalMinutes
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return defaultUpstreamBillingProbeIntervalMinutes
	}
	h.ensureUpstreamBillingProbeDefaultsLocked()
	return h.cfg.UpstreamBillingProbe.IntervalMinutes
}

func (h *Handler) upstreamBillingProbeSettingsPayload() upstreamBillingProbeSettings {
	if h == nil {
		return upstreamBillingProbeSettings{
			IntervalMinutes:     defaultUpstreamBillingProbeIntervalMinutes,
			HealthEnabled:       false,
			HealthModel:         defaultUpstreamHealthProbeModel,
			AutoPriorityEnabled: false,
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return upstreamBillingProbeSettings{
			IntervalMinutes:     defaultUpstreamBillingProbeIntervalMinutes,
			HealthEnabled:       false,
			HealthModel:         defaultUpstreamHealthProbeModel,
			AutoPriorityEnabled: false,
		}
	}
	h.ensureUpstreamBillingProbeDefaultsLocked()
	return upstreamBillingProbeSettings{
		IntervalMinutes:     h.cfg.UpstreamBillingProbe.IntervalMinutes,
		HealthEnabled:       h.cfg.UpstreamBillingProbe.HealthEnabled,
		HealthModel:         h.cfg.UpstreamBillingProbe.HealthModel,
		AutoPriorityEnabled: h.cfg.UpstreamBillingProbe.AutoPriorityEnabled,
	}
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

// isEligibleUpstreamAuth accepts configuration-backed API keys from every
// native protocol. OAuth/file-backed credentials do not expose a stable
// upstream api_key and are intentionally left out.
func isEligibleUpstreamAuth(auth *coreauth.Auth) bool {
	if auth == nil || auth.Disabled || auth.Status == coreauth.StatusDisabled {
		return false
	}
	return strings.TrimSpace(auth.Attributes["api_key"]) != ""
}

func isEligibleUpstreamBillingAuth(auth *coreauth.Auth) bool {
	return isEligibleUpstreamAuth(auth) && strings.TrimSpace(auth.Attributes["base_url"]) != ""
}

func (h *Handler) startUpstreamBillingProbeLoop() {
	go func() {
		// ponytail: one background loop is enough; add lifecycle stop only if handlers become short-lived.
		for {
			h.dispatchUpstreamProbes()
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
	if !isEligibleUpstreamAuth(auth) {
		return upstreamBillingProbeEntry{}, false
	}
	if auth.Disabled || auth.Status == coreauth.StatusDisabled {
		return upstreamBillingProbeEntry{}, false
	}
	key := strings.TrimSpace(auth.Attributes["api_key"])
	baseURL := strings.TrimSpace(auth.Attributes["base_url"])
	if key == "" {
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
				cached.Provider = entry.Provider
				cached.ProviderName = entry.ProviderName
				cached.BaseURL = entry.BaseURL
				cached.APIKeyPreview = entry.APIKeyPreview
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
		for i := range copyCache.entries {
			copyCache.entries[i].HealthHistory = append(
				[]upstreamHealthProbeSample(nil),
				copyCache.entries[i].HealthHistory...,
			)
		}
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

func (h *Handler) storeUpstreamBillingProbeResult(entry upstreamBillingProbeEntry) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.upstreamBillingProbeCache == nil {
		h.upstreamBillingProbeCache = &upstreamBillingProbeCache{}
	}
	for i := range h.upstreamBillingProbeCache.entries {
		previous := h.upstreamBillingProbeCache.entries[i]
		if previous.AuthIndex != entry.AuthIndex {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(entry.Status), "ok") &&
			strings.EqualFold(strings.TrimSpace(previous.Status), "ok") {
			return
		}
		entry.HealthHistory = append([]upstreamHealthProbeSample(nil), previous.HealthHistory...)
		h.upstreamBillingProbeCache.entries[i] = entry
		h.upstreamBillingProbeCache.updatedAt = time.Now().UTC()
		return
	}
	h.upstreamBillingProbeCache.entries = append(h.upstreamBillingProbeCache.entries, entry)
	h.upstreamBillingProbeCache.updatedAt = time.Now().UTC()
}

func (h *Handler) storeUpstreamHealthProbeResult(auth *coreauth.Auth, sample upstreamHealthProbeSample) {
	if h == nil || auth == nil {
		return
	}
	authIndex := strings.TrimSpace(auth.EnsureIndex())
	if authIndex == "" {
		return
	}
	providerName := strings.TrimSpace(auth.Label)
	if providerName == "" {
		providerName = strings.TrimSpace(auth.Attributes["compat_name"])
	}
	if providerName == "" {
		providerName = "openai-compatibility"
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.upstreamBillingProbeCache == nil {
		h.upstreamBillingProbeCache = &upstreamBillingProbeCache{}
	}
	for i := range h.upstreamBillingProbeCache.entries {
		entry := &h.upstreamBillingProbeCache.entries[i]
		if entry.AuthIndex != authIndex {
			continue
		}
		entry.HealthHistory = appendHealthProbeSample(entry.HealthHistory, &sample)
		h.upstreamBillingProbeCache.updatedAt = time.Now().UTC()
		return
	}
	h.upstreamBillingProbeCache.entries = append(h.upstreamBillingProbeCache.entries, upstreamBillingProbeEntry{
		AuthIndex:     authIndex,
		Provider:      strings.ToLower(strings.TrimSpace(auth.Provider)),
		ProviderName:  providerName,
		BaseURL:       strings.TrimSpace(auth.Attributes["base_url"]),
		APIKeyPreview: util.HideAPIKey(strings.TrimSpace(auth.Attributes["api_key"])),
		Status:        "skipped",
		HealthHistory: []upstreamHealthProbeSample{sample},
	})
	h.upstreamBillingProbeCache.updatedAt = time.Now().UTC()
}

func firstHealthProbeSample(samples []upstreamHealthProbeSample) *upstreamHealthProbeSample {
	if len(samples) == 0 {
		return nil
	}
	sample := samples[len(samples)-1]
	return &sample
}

func appendHealthProbeSample(history []upstreamHealthProbeSample, sample *upstreamHealthProbeSample) []upstreamHealthProbeSample {
	next := append([]upstreamHealthProbeSample(nil), history...)
	if sample != nil && !sample.CheckedAt.IsZero() {
		next = append(next, *sample)
	}
	if len(next) > upstreamHealthProbeHistoryLimit {
		next = next[len(next)-upstreamHealthProbeHistoryLimit:]
	}
	return next
}

func (h *Handler) GetUpstreamBillingProbe(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusOK, upstreamBillingProbePayload{
			Settings: upstreamBillingProbeSettings{IntervalMinutes: defaultUpstreamBillingProbeIntervalMinutes, HealthEnabled: false, HealthModel: defaultUpstreamHealthProbeModel, AutoPriorityEnabled: false},
			Items:    []upstreamBillingProbeEntry{},
			Running:  []upstreamProbeTaskRef{},
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
		Running:   h.upstreamProbeRunningSnapshot(),
	})
}

func (h *Handler) PutUpstreamBillingProbe(c *gin.Context) {
	var body struct {
		ManualAuthIndex      *string  `json:"auth-index"`
		ManualProvider       *string  `json:"provider"`
		ManualBaseURL        *string  `json:"base-url"`
		ManualAPIKey         *string  `json:"api-key"`
		ManualRateMultiplier *float64 `json:"effective-rate-multiplier"`
		IntervalMinutes      *int     `json:"interval-minutes"`
		HealthEnabled        *bool    `json:"health-enabled"`
		HealthModel          *string  `json:"health-model"`
		AutoPriorityEnabled  *bool    `json:"auto-priority-enabled"`
		Value                *struct {
			IntervalMinutes     *int    `json:"interval-minutes"`
			HealthEnabled       *bool   `json:"health-enabled"`
			HealthModel         *string `json:"health-model"`
			AutoPriorityEnabled *bool   `json:"auto-priority-enabled"`
		} `json:"value"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if body.ManualAuthIndex != nil || body.ManualAPIKey != nil || body.ManualRateMultiplier != nil {
		if body.ManualRateMultiplier == nil || *body.ManualRateMultiplier < 0 || math.IsNaN(*body.ManualRateMultiplier) || math.IsInf(*body.ManualRateMultiplier, 0) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid manual multiplier"})
			return
		}
		authIndex := ""
		if body.ManualAuthIndex != nil {
			authIndex = strings.TrimSpace(*body.ManualAuthIndex)
		}
		h.mu.Lock()
		manager := h.authManager
		h.mu.Unlock()
		var auth *coreauth.Auth
		if manager != nil {
			for _, candidate := range manager.List() {
				if candidate != nil && authIndex != "" && strings.TrimSpace(candidate.EnsureIndex()) == authIndex {
					auth = candidate
					break
				}
			}
			if auth == nil {
				auth = findManualUpstreamRateAuth(manager.List(), body.ManualProvider, body.ManualBaseURL, body.ManualAPIKey)
			}
		}
		if auth == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "upstream auth not found"})
			return
		}
		authIndex = strings.TrimSpace(auth.EnsureIndex())
		entry, _ := h.upstreamBillingProbeEntryForAuth(auth)
		value := *body.ManualRateMultiplier
		entry.Status = "ok"
		entry.Error = ""
		entry.ObservedAt = time.Now().UTC()
		entry.GroupRateMultiplier = &value
		entry.ResolvedRateMultiplier = &value
		entry.EffectiveRateMultiplier = &value
		h.storeUpstreamBillingProbeResult(entry)
		stored, _ := h.previousUpstreamBillingProbeEntry(authIndex)
		c.JSON(http.StatusOK, stored)
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
	healthEnabled := h.upstreamHealthProbeEnabled()
	if body.HealthEnabled != nil {
		healthEnabled = *body.HealthEnabled
	}
	if body.Value != nil && body.Value.HealthEnabled != nil {
		healthEnabled = *body.Value.HealthEnabled
	}
	autoPriorityEnabled := h.upstreamAutoPriorityEnabled()
	if body.AutoPriorityEnabled != nil {
		autoPriorityEnabled = *body.AutoPriorityEnabled
	}
	if body.Value != nil && body.Value.AutoPriorityEnabled != nil {
		autoPriorityEnabled = *body.Value.AutoPriorityEnabled
	}
	healthModel := ""
	if body.HealthModel != nil {
		healthModel = strings.TrimSpace(*body.HealthModel)
	}
	if body.Value != nil && body.Value.HealthModel != nil {
		healthModel = strings.TrimSpace(*body.Value.HealthModel)
	}
	if healthModel == "" {
		healthModel = defaultUpstreamHealthProbeModel
	}
	h.mu.Lock()
	if h.cfg != nil {
		h.cfg.UpstreamBillingProbe.IntervalMinutes = interval
		h.cfg.UpstreamBillingProbe.HealthEnabled = healthEnabled
		h.cfg.UpstreamBillingProbe.HealthModel = healthModel
		h.cfg.UpstreamBillingProbe.AutoPriorityEnabled = autoPriorityEnabled
	}
	ok := h.persistLocked(c)
	h.mu.Unlock()
	if ok {
		h.dispatchUpstreamProbes()
	}
}

func findManualUpstreamRateAuth(auths []*coreauth.Auth, provider, baseURL, apiKey *string) *coreauth.Auth {
	if apiKey == nil || strings.TrimSpace(*apiKey) == "" {
		return nil
	}
	wantProvider := ""
	if provider != nil {
		wantProvider = strings.ToLower(strings.TrimSpace(*provider))
	}
	wantBaseURL := ""
	if baseURL != nil {
		wantBaseURL = strings.TrimRight(strings.ToLower(strings.TrimSpace(*baseURL)), "/")
	}
	for _, auth := range auths {
		if auth == nil || strings.TrimSpace(auth.Attributes["api_key"]) != strings.TrimSpace(*apiKey) {
			continue
		}
		if wantBaseURL != "" && strings.TrimRight(strings.ToLower(strings.TrimSpace(auth.Attributes["base_url"])), "/") != wantBaseURL {
			continue
		}
		runtimeProvider := strings.ToLower(strings.TrimSpace(auth.Provider))
		switch wantProvider {
		case "", runtimeProvider:
		case "interactions":
			if runtimeProvider != "gemini-interactions" {
				continue
			}
		case "claudeapi":
			if runtimeProvider != "claude" {
				continue
			}
		case "openaicompatibility":
			if !isOpenAICompatibilityAuth(auth) {
				continue
			}
		default:
			continue
		}
		return auth
	}
	return nil
}

func (h *Handler) RefreshUpstreamBilling(c *gin.Context) {
	result := h.dispatchUpstreamProbes()
	c.JSON(http.StatusAccepted, result)
}

func (h *Handler) dispatchUpstreamProbes() upstreamProbeDispatchResult {
	result := upstreamProbeDispatchResult{
		Accepted: []upstreamProbeTaskRef{},
		Skipped:  []upstreamProbeSkippedTask{},
		Running:  []upstreamProbeTaskRef{},
	}
	if h == nil {
		return result
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		return result
	}

	healthEnabled := h.upstreamHealthProbeEnabled()
	healthModel := h.upstreamHealthProbeModel()
	works := make([]upstreamProbeWork, 0)
	valid := make(map[upstreamProbeTaskKey][32]byte)
	for _, auth := range manager.List() {
		if !isEligibleUpstreamProbeAuth(auth) {
			continue
		}
		authIndex := strings.TrimSpace(auth.EnsureIndex())
		if authIndex == "" {
			continue
		}
		if isEligibleUpstreamBillingAuth(auth) {
			billingKey := upstreamProbeTaskKey{AuthIndex: authIndex, Kind: upstreamProbeBilling}
			billingFingerprint := upstreamProbeFingerprint(auth, upstreamProbeBilling, "")
			valid[billingKey] = billingFingerprint
			works = append(works, upstreamProbeWork{Key: billingKey, Auth: auth, Fingerprint: billingFingerprint})
		}
		if healthEnabled {
			healthKey := upstreamProbeTaskKey{AuthIndex: authIndex, Kind: upstreamProbeHealth}
			healthFingerprint := upstreamProbeFingerprint(auth, upstreamProbeHealth, healthModel)
			valid[healthKey] = healthFingerprint
			works = append(works, upstreamProbeWork{
				Key:         healthKey,
				Auth:        auth,
				HealthModel: healthModel,
				Fingerprint: healthFingerprint,
			})
		}
	}
	h.cancelStaleUpstreamProbeTasks(valid)

	for _, work := range works {
		token, taskCtx, started := h.tryStartUpstreamProbeTask(work.Key, work.Fingerprint)
		ref := upstreamProbeTaskRef{AuthIndex: work.Key.AuthIndex, Kind: work.Key.Kind}
		if !started {
			result.Skipped = append(result.Skipped, upstreamProbeSkippedTask{
				upstreamProbeTaskRef: ref,
				Reason:               "already-running",
			})
			continue
		}
		result.Accepted = append(result.Accepted, ref)
		go h.runUpstreamProbeTask(taskCtx, token, work)
	}
	result.Running = h.upstreamProbeRunningSnapshot()
	return result
}

func isEligibleUpstreamProbeAuth(auth *coreauth.Auth) bool {
	if !isEligibleUpstreamAuth(auth) {
		return false
	}
	if strings.TrimSpace(auth.Attributes["base_url"]) != "" {
		return true
	}
	// These native API-key protocols have stable official defaults.
	switch strings.ToLower(strings.TrimSpace(auth.Provider)) {
	case "claude", "gemini", "gemini-interactions", "codex", "xai", "vertex":
		return true
	default:
		return false
	}
}

func upstreamProbeFingerprint(auth *coreauth.Auth, kind upstreamProbeKind, healthModel string) [32]byte {
	if auth == nil {
		return [32]byte{}
	}
	parts := []string{
		strings.TrimSpace(auth.ID),
		strings.TrimSpace(auth.EnsureIndex()),
		strings.ToLower(strings.TrimSpace(auth.Provider)),
		strings.TrimSpace(auth.Attributes["base_url"]),
		strings.TrimSpace(auth.Attributes["api_key"]),
		strings.TrimSpace(auth.Attributes["models_hash"]),
		strings.TrimSpace(auth.ProxyURL),
		string(kind),
	}
	if len(auth.Attributes) > 0 {
		headerKeys := make([]string, 0)
		for key := range auth.Attributes {
			if strings.HasPrefix(key, "header:") {
				headerKeys = append(headerKeys, key)
			}
		}
		sort.Strings(headerKeys)
		for _, key := range headerKeys {
			parts = append(parts, key, strings.TrimSpace(auth.Attributes[key]))
		}
	}
	if kind == upstreamProbeHealth {
		parts = append(parts, strings.TrimSpace(healthModel))
	}
	return sha256.Sum256([]byte(strings.Join(parts, "\x00")))
}

func (h *Handler) tryStartUpstreamProbeTask(key upstreamProbeTaskKey, fingerprint [32]byte) (uint64, context.Context, bool) {
	taskCtx, cancel := context.WithCancel(context.Background())
	h.upstreamProbeMu.Lock()
	defer h.upstreamProbeMu.Unlock()
	if h.upstreamProbeInFlight == nil {
		h.upstreamProbeInFlight = make(map[upstreamProbeTaskKey]upstreamProbeTask)
	}
	if current, ok := h.upstreamProbeInFlight[key]; ok {
		if current.Fingerprint == fingerprint {
			cancel()
			return 0, nil, false
		}
		current.Cancel()
	}
	h.upstreamProbeNextToken++
	token := h.upstreamProbeNextToken
	h.upstreamProbeInFlight[key] = upstreamProbeTask{
		Token:       token,
		Fingerprint: fingerprint,
		StartedAt:   time.Now().UTC(),
		Cancel:      cancel,
	}
	return token, taskCtx, true
}

func (h *Handler) cancelStaleUpstreamProbeTasks(valid map[upstreamProbeTaskKey][32]byte) {
	h.upstreamProbeMu.Lock()
	defer h.upstreamProbeMu.Unlock()
	for key, task := range h.upstreamProbeInFlight {
		fingerprint, ok := valid[key]
		if ok && fingerprint == task.Fingerprint {
			continue
		}
		task.Cancel()
		delete(h.upstreamProbeInFlight, key)
	}
}

func (h *Handler) upstreamProbeTaskIsCurrent(key upstreamProbeTaskKey, token uint64, fingerprint [32]byte) bool {
	h.upstreamProbeMu.Lock()
	defer h.upstreamProbeMu.Unlock()
	task, ok := h.upstreamProbeInFlight[key]
	return ok && task.Token == token && task.Fingerprint == fingerprint
}

func (h *Handler) finishUpstreamProbeTask(key upstreamProbeTaskKey, token uint64) {
	h.upstreamProbeMu.Lock()
	defer h.upstreamProbeMu.Unlock()
	if task, ok := h.upstreamProbeInFlight[key]; ok && task.Token == token {
		delete(h.upstreamProbeInFlight, key)
	}
}

func (h *Handler) upstreamProbeRunningSnapshot() []upstreamProbeTaskRef {
	if h == nil {
		return []upstreamProbeTaskRef{}
	}
	h.upstreamProbeMu.Lock()
	running := make([]upstreamProbeTaskRef, 0, len(h.upstreamProbeInFlight))
	for key := range h.upstreamProbeInFlight {
		running = append(running, upstreamProbeTaskRef{AuthIndex: key.AuthIndex, Kind: key.Kind})
	}
	h.upstreamProbeMu.Unlock()
	sort.SliceStable(running, func(i, j int) bool {
		if running[i].AuthIndex != running[j].AuthIndex {
			return running[i].AuthIndex < running[j].AuthIndex
		}
		return running[i].Kind < running[j].Kind
	})
	return running
}

func (h *Handler) runUpstreamProbeTask(ctx context.Context, token uint64, work upstreamProbeWork) {
	defer func() {
		h.finishUpstreamProbeTask(work.Key, token)
		if recovered := recover(); recovered != nil {
			log.WithFields(log.Fields{
				"auth_index": work.Key.AuthIndex,
				"kind":       work.Key.Kind,
				"panic":      recovered,
			}).Error("management: upstream probe task panicked")
		}
	}()
	if work.Key.Kind == upstreamProbeBilling {
		entry, ok := h.probeUpstreamBillingEntry(ctx, work.Auth)
		if ok && h.upstreamProbeResultIsCurrent(work, token) {
			h.storeUpstreamBillingProbeResult(entry)
		}
		return
	}
	sample, ok := h.probeUpstreamHealth(ctx, work.Auth, work.HealthModel)
	if !ok || !h.upstreamProbeResultIsCurrent(work, token) {
		return
	}
	h.storeUpstreamHealthProbeResult(work.Auth, sample)
	if h.upstreamAutoPriorityEnabled() {
		h.applyUpstreamHealthPriorities(context.Background(), []upstreamBillingProbeEntry{{
			AuthIndex:     work.Key.AuthIndex,
			HealthHistory: []upstreamHealthProbeSample{sample},
		}})
	}
}

func (h *Handler) upstreamProbeResultIsCurrent(work upstreamProbeWork, token uint64) bool {
	if !h.upstreamProbeTaskIsCurrent(work.Key, token, work.Fingerprint) {
		return false
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		return false
	}
	if work.Key.Kind == upstreamProbeHealth {
		if !h.upstreamHealthProbeEnabled() || h.upstreamHealthProbeModel() != work.HealthModel {
			return false
		}
	}
	for _, auth := range manager.List() {
		if auth == nil || strings.TrimSpace(auth.EnsureIndex()) != work.Key.AuthIndex {
			continue
		}
		return upstreamProbeFingerprint(auth, work.Key.Kind, work.HealthModel) == work.Fingerprint
	}
	return false
}

func (h *Handler) upstreamHealthProbeEnabled() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return false
	}
	h.ensureUpstreamBillingProbeDefaultsLocked()
	return h.cfg.UpstreamBillingProbe.HealthEnabled
}

func (h *Handler) upstreamAutoPriorityEnabled() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return false
	}
	return h.cfg.UpstreamBillingProbe.AutoPriorityEnabled
}

func (h *Handler) previousUpstreamBillingProbeEntry(authIndex string) (upstreamBillingProbeEntry, bool) {
	if h == nil || strings.TrimSpace(authIndex) == "" {
		return upstreamBillingProbeEntry{}, false
	}
	cache := h.loadUpstreamBillingProbeCache()
	if cache == nil {
		return upstreamBillingProbeEntry{}, false
	}
	for _, entry := range cache.entries {
		if entry.AuthIndex == authIndex {
			return entry, true
		}
	}
	return upstreamBillingProbeEntry{}, false
}

func (h *Handler) upstreamHealthProbeModel() string {
	if h == nil {
		return defaultUpstreamHealthProbeModel
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return defaultUpstreamHealthProbeModel
	}
	h.ensureUpstreamBillingProbeDefaultsLocked()
	return h.cfg.UpstreamBillingProbe.HealthModel
}

func upstreamHealthPriority(sample upstreamHealthProbeSample) int {
	if !strings.EqualFold(strings.TrimSpace(sample.Status), "ok") {
		return upstreamHealthFailedPriority
	}
	if sample.LatencyMS > upstreamHealthFastLatencyMS {
		return upstreamHealthNormalPriority
	}
	return upstreamHealthExcellentPriority
}

func (h *Handler) applyUpstreamHealthPriorities(ctx context.Context, entries []upstreamBillingProbeEntry) {
	if h == nil || len(entries) == 0 {
		return
	}
	priorityByAuthIndex := make(map[string]int, len(entries))
	for _, entry := range entries {
		if authIndex := strings.TrimSpace(entry.AuthIndex); authIndex != "" {
			if sample := firstHealthProbeSample(entry.HealthHistory); sample != nil {
				priorityByAuthIndex[authIndex] = upstreamHealthPriority(*sample)
			}
		}
	}
	if len(priorityByAuthIndex) == 0 {
		return
	}
	liveIndexByID := h.liveAuthIndexByID()
	type nativePriorityTarget struct {
		provider    string
		configIndex int
	}
	nativeTargets := make(map[string]nativePriorityTarget)
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager != nil {
		for _, auth := range manager.List() {
			if auth == nil {
				continue
			}
			provider := strings.ToLower(strings.TrimSpace(auth.Provider))
			if provider != "gemini" && provider != "gemini-interactions" && provider != "claude" && provider != "codex" && provider != "xai" && provider != "vertex" {
				continue
			}
			idx, err := strconv.Atoi(strings.TrimSpace(auth.Attributes["config_index"]))
			if err != nil || idx < 0 {
				continue
			}
			nativeTargets[strings.TrimSpace(auth.EnsureIndex())] = nativePriorityTarget{provider: provider, configIndex: idx}
		}
	}

	h.mu.Lock()
	if h.cfg == nil || !h.cfg.UpstreamBillingProbe.AutoPriorityEnabled {
		h.mu.Unlock()
		return
	}
	changed := false
	for authIndex, priority := range priorityByAuthIndex {
		target, ok := nativeTargets[authIndex]
		if !ok {
			continue
		}
		switch target.provider {
		case "gemini":
			if target.configIndex < len(h.cfg.GeminiKey) && h.cfg.GeminiKey[target.configIndex].Priority != priority {
				h.cfg.GeminiKey[target.configIndex].Priority = priority
				changed = true
			}
		case "gemini-interactions":
			if target.configIndex < len(h.cfg.InteractionsKey) && h.cfg.InteractionsKey[target.configIndex].Priority != priority {
				h.cfg.InteractionsKey[target.configIndex].Priority = priority
				changed = true
			}
		case "claude":
			if target.configIndex < len(h.cfg.ClaudeKey) && h.cfg.ClaudeKey[target.configIndex].Priority != priority {
				h.cfg.ClaudeKey[target.configIndex].Priority = priority
				changed = true
			}
		case "codex":
			if target.configIndex < len(h.cfg.CodexKey) && h.cfg.CodexKey[target.configIndex].Priority != priority {
				h.cfg.CodexKey[target.configIndex].Priority = priority
				changed = true
			}
		case "xai":
			if target.configIndex < len(h.cfg.XAIKey) && h.cfg.XAIKey[target.configIndex].Priority != priority {
				h.cfg.XAIKey[target.configIndex].Priority = priority
				changed = true
			}
		case "vertex":
			if target.configIndex < len(h.cfg.VertexCompatAPIKey) && h.cfg.VertexCompatAPIKey[target.configIndex].Priority != priority {
				h.cfg.VertexCompatAPIKey[target.configIndex].Priority = priority
				changed = true
			}
		}
	}
	normalized := normalizedOpenAICompatibilityEntries(h.cfg.OpenAICompatibility)
	idGen := synthesizer.NewStableIDGenerator()
	for i := range normalized {
		entry := normalized[i]
		providerName := strings.ToLower(strings.TrimSpace(entry.Name))
		if providerName == "" {
			providerName = "openai-compatibility"
		}
		idKind := fmt.Sprintf("openai-compatibility:%s", providerName)
		baseURL := strings.TrimSpace(entry.BaseURL)
		var matched *int
		if len(entry.APIKeyEntries) == 0 {
			id, _ := idGen.Next(idKind, baseURL)
			if priority, ok := priorityByAuthIndex[liveIndexByID[id]]; ok {
				matched = &priority
			}
		} else {
			for j := range entry.APIKeyEntries {
				apiKeyEntry := entry.APIKeyEntries[j]
				id, _ := idGen.Next(idKind, apiKeyEntry.APIKey, baseURL, apiKeyEntry.ProxyURL)
				if priority, ok := priorityByAuthIndex[liveIndexByID[id]]; ok {
					matched = &priority
					break
				}
			}
		}
		if matched == nil || h.cfg.OpenAICompatibility[i].Priority == *matched {
			continue
		}
		h.cfg.OpenAICompatibility[i].Priority = *matched
		changed = true
	}
	if !changed {
		h.mu.Unlock()
		return
	}
	if h.configFilePath != "" {
		if errSave := config.SaveConfigPreserveComments(h.configFilePath, h.cfg); errSave != nil {
			h.mu.Unlock()
			return
		}
	}
	snapshot := h.reloadSnapshotConfigLocked()
	h.mu.Unlock()
	reloadCtx := context.Background()
	if ctx != nil {
		reloadCtx = context.WithoutCancel(ctx)
	}
	h.reloadConfigAfterManagementSave(reloadCtx, snapshot)
}

func (h *Handler) probeUpstreamHealth(ctx context.Context, auth *coreauth.Auth, model string) (upstreamHealthProbeSample, bool) {
	if h == nil || !isEligibleUpstreamAuth(auth) {
		return upstreamHealthProbeSample{}, false
	}
	if auth.Disabled || auth.Status == coreauth.StatusDisabled {
		return upstreamHealthProbeSample{}, false
	}
	key := strings.TrimSpace(auth.Attributes["api_key"])
	baseURL := strings.TrimSpace(auth.Attributes["base_url"])
	model = h.resolveUpstreamHealthProbeModel(auth, model)
	if key == "" {
		return upstreamHealthProbeSample{}, false
	}
	sample := upstreamHealthProbeSample{Status: "failed", Model: model, CheckedAt: time.Now().UTC()}
	reqURL, protocol := buildUpstreamHealthProbeURL(auth, baseURL, model)
	challenge := newUpstreamHealthProbeChallenge()
	bodyPayload := upstreamHealthProbePayloadForProtocol(protocol, model, challenge)
	body, err := json.Marshal(bodyPayload)
	if err != nil {
		sample.Error = "request-build-failed"
		return sample, true
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, strings.NewReader(string(body)))
	if err != nil {
		sample.Error = "request-build-failed"
		return sample, true
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	setUpstreamHealthProbeHeaders(req, auth, key, protocol)
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	client := &http.Client{}
	if auth.ProxyURL != "" {
		if proxyURL, errParse := url.Parse(auth.ProxyURL); errParse == nil && proxyURL != nil {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.Proxy = http.ProxyURL(proxyURL)
			client.Transport = transport
			defer transport.CloseIdleConnections()
		}
	}
	started := time.Now()
	resp, errDo := client.Do(req)
	if errDo != nil {
		sample.LatencyMS = time.Since(started).Milliseconds()
		sample.Error = "request-failed"
		return sample, true
	}
	defer func() { _ = resp.Body.Close() }()
	sample.HTTPStatus = resp.StatusCode
	responseBody, errRead := io.ReadAll(io.LimitReader(resp.Body, upstreamBillingProbeMaxBodyBytes+1))
	sample.LatencyMS = time.Since(started).Milliseconds()
	if errRead != nil {
		sample.Error = "response-read-failed"
		return sample, true
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		sample.Error = fmt.Sprintf("http-%d", resp.StatusCode)
		return sample, true
	}
	if len(responseBody) > upstreamBillingProbeMaxBodyBytes || !isSuccessfulUpstreamHealthProbeResponseForProtocol(responseBody, challenge.Expected, protocol) {
		sample.Error = "unexpected-response"
		return sample, true
	}
	// Prompt-cache usage is accounting metadata, not a health failure. Some
	// compatibility upstreams inject a stable prefix and legitimately report
	// cached input tokens even for a fresh probe request.
	sample.CachedTokens = upstreamHealthProbeCachedTokens(responseBody)
	sample.Status = "ok"
	return sample, true
}

func (h *Handler) resolveUpstreamHealthProbeModel(auth *coreauth.Auth, fallback string) string {
	fallback = strings.TrimSpace(fallback)
	provider := strings.ToLower(strings.TrimSpace(auth.Provider))
	idx, _ := strconv.Atoi(strings.TrimSpace(auth.Attributes["config_index"]))
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg != nil && idx >= 0 {
		var name string
		switch provider {
		case "gemini":
			if idx < len(h.cfg.GeminiKey) && len(h.cfg.GeminiKey[idx].Models) > 0 {
				name = h.cfg.GeminiKey[idx].Models[0].Name
			}
		case "gemini-interactions":
			if idx < len(h.cfg.InteractionsKey) && len(h.cfg.InteractionsKey[idx].Models) > 0 {
				name = h.cfg.InteractionsKey[idx].Models[0].Name
			}
		case "claude":
			if idx < len(h.cfg.ClaudeKey) && len(h.cfg.ClaudeKey[idx].Models) > 0 {
				name = h.cfg.ClaudeKey[idx].Models[0].Name
			}
		case "codex":
			if idx < len(h.cfg.CodexKey) && len(h.cfg.CodexKey[idx].Models) > 0 {
				name = h.cfg.CodexKey[idx].Models[0].Name
			}
		case "xai":
			if idx < len(h.cfg.XAIKey) && len(h.cfg.XAIKey[idx].Models) > 0 {
				name = h.cfg.XAIKey[idx].Models[0].Name
			}
		case "vertex":
			if idx < len(h.cfg.VertexCompatAPIKey) && len(h.cfg.VertexCompatAPIKey[idx].Models) > 0 {
				name = h.cfg.VertexCompatAPIKey[idx].Models[0].Name
			}
		}
		if name = strings.TrimSpace(name); name != "" {
			return name
		}
	}
	if fallback != "" {
		return fallback
	}
	switch provider {
	case "claude":
		return "claude-3-5-haiku-latest"
	case "gemini", "gemini-interactions", "vertex":
		return "gemini-2.5-flash"
	case "xai":
		return "grok-3-mini"
	}
	return defaultUpstreamHealthProbeModel
}

func upstreamProtocol(auth *coreauth.Auth) string {
	if auth == nil {
		return "openai"
	}
	switch strings.ToLower(strings.TrimSpace(auth.Provider)) {
	case "codex":
		return "responses"
	case "xai":
		return "xai-responses"
	case "claude":
		return "claude"
	case "gemini":
		return "gemini"
	case "gemini-interactions":
		return "interactions"
	case "vertex":
		return "vertex"
	default:
		return "openai"
	}
}

func buildUpstreamHealthProbeURL(auth *coreauth.Auth, baseURL, model string) (string, string) {
	protocol := upstreamProtocol(auth)
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if base == "" {
		switch protocol {
		case "responses":
			base = "https://chatgpt.com/backend-api/codex"
		case "xai-responses":
			base = "https://api.x.ai/v1"
		case "claude":
			base = "https://api.anthropic.com"
		case "gemini", "interactions":
			base = "https://generativelanguage.googleapis.com"
		case "vertex":
			base = "https://aiplatform.googleapis.com"
		}
	}
	switch protocol {
	case "responses", "xai-responses":
		return appendUpstreamPath(base, "/responses"), protocol
	case "claude":
		return appendUpstreamPath(base, "/v1/messages"), protocol
	case "gemini":
		return appendUpstreamPath(base, "/v1beta/models/"+url.PathEscape(model)+":generateContent"), protocol
	case "interactions":
		return appendUpstreamPath(base, "/v1beta/interactions"), protocol
	case "vertex":
		return appendUpstreamPath(base, "/v1/publishers/google/models/"+url.PathEscape(model)+":generateContent"), protocol
	default:
		u, _ := buildOpenAICompatibilityChatCompletionsURL(baseURL)
		return u, protocol
	}
}

func appendUpstreamPath(base, endpoint string) string {
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	endpoint = "/" + strings.TrimLeft(endpoint, "/")
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" {
		return base + endpoint
	}
	basePath := strings.TrimRight(parsed.Path, "/")
	for _, prefix := range []string{"/v1", "/v1beta"} {
		if strings.HasPrefix(endpoint, prefix+"/") && strings.HasSuffix(basePath, prefix) {
			endpoint = strings.TrimPrefix(endpoint, prefix)
			break
		}
	}
	parsed.Path = strings.TrimRight(basePath, "/") + endpoint
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func upstreamHealthProbePayloadForProtocol(protocol, model string, challenge upstreamHealthProbeChallenge) any {
	switch protocol {
	case "responses", "xai-responses":
		return map[string]any{"model": model, "input": []any{map[string]any{"type": "message", "role": "user", "content": []any{map[string]string{"type": "input_text", "text": challenge.Prompt}}}}, "instructions": upstreamHealthProbeInstructions, "max_output_tokens": upstreamHealthProbeMaxTokens, "stream": false, "store": false}
	case "claude":
		return map[string]any{"model": model, "max_tokens": upstreamHealthProbeMaxTokens, "messages": []any{map[string]any{"role": "user", "content": challenge.Prompt}}}
	case "gemini", "vertex":
		return map[string]any{"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]string{"text": challenge.Prompt}}}}, "generationConfig": map[string]any{"maxOutputTokens": upstreamHealthProbeMaxTokens}}
	case "interactions":
		return map[string]any{"agent": model, "input": challenge.Prompt, "stream": false}
	default:
		return upstreamHealthProbePayload(model, challenge)
	}
}

func setUpstreamHealthProbeHeaders(req *http.Request, auth *coreauth.Auth, key, protocol string) {
	switch protocol {
	case "claude":
		req.Header.Set("x-api-key", key)
		req.Header.Set("anthropic-version", "2023-06-01")
	case "gemini", "interactions":
		req.Header.Set("x-goog-api-key", key)
		if protocol == "interactions" {
			req.Header.Set("Api-Revision", "2026-05-20")
		}
	case "vertex":
		req.Header.Set("x-goog-api-key", key)
	default:
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if auth != nil {
		for name, value := range auth.Attributes {
			if strings.HasPrefix(name, "header:") {
				req.Header.Set(strings.TrimPrefix(name, "header:"), value)
			}
		}
	}
}

func newUpstreamHealthProbeChallenge() upstreamHealthProbeChallenge {
	id := uuid.New()
	left := int(id[0]%upstreamHealthProbeOperandMax) + 1
	right := int(id[1]%upstreamHealthProbeOperandMax) + 1
	return upstreamHealthProbeChallenge{
		Prompt:   fmt.Sprintf("%d + %d = ?", left, right),
		Expected: fmt.Sprintf("%d", left+right),
		CacheKey: "cliproxy-health-" + id.String(),
	}
}

// A short, non-empty instructions field prevents Sub2API's Chat Completions
// bridge from falling back to the long Codex instructions prefix. Keeping the
// full prompt below the model cache threshold makes every probe uncached.
func upstreamHealthProbePayload(model string, challenge upstreamHealthProbeChallenge) map[string]any {
	return map[string]any{
		"model":            model,
		"instructions":     upstreamHealthProbeInstructions,
		"prompt_cache_key": challenge.CacheKey,
		"messages": []map[string]string{
			{"role": "user", "content": challenge.Prompt},
		},
		"max_tokens": upstreamHealthProbeMaxTokens,
		"stream":     false,
	}
}

func isSuccessfulUpstreamHealthProbeResponse(body []byte, _ string) bool {
	return strings.TrimSpace(upstreamHealthProbeResponseText(body)) != ""
}

func isSuccessfulUpstreamHealthProbeResponseForProtocol(body []byte, expected, protocol string) bool {
	if protocol == "openai" {
		return isSuccessfulUpstreamHealthProbeResponse(body, expected)
	}
	var text string
	paths := map[string][]string{
		"responses":     {"output_text", "output.#.content.#.text", "output.#.content.#", "output.#.text"},
		"xai-responses": {"output_text", "output.#.content.#.text", "output.#.content.#", "output.#.text"},
		"claude":        {"content.#.text"},
		"gemini":        {"candidates.#.content.parts.#.text"},
		"vertex":        {"candidates.#.content.parts.#.text"},
		"interactions":  {"steps.#.content.#.text", "output.#.content.#.text", "output_text"},
	}
	for _, path := range paths[protocol] {
		if value := gjson.GetBytes(body, path); value.Exists() {
			text += " " + value.String()
		}
	}
	return strings.TrimSpace(text) != ""
}

func upstreamHealthProbeResponseText(body []byte) string {
	content := gjson.GetBytes(body, "choices.0.message.content")
	if !content.Exists() {
		return ""
	}
	if content.Type == gjson.String {
		return strings.TrimSpace(content.String())
	}
	if content.IsArray() {
		var textParts []string
		content.ForEach(func(_, part gjson.Result) bool {
			if text := strings.TrimSpace(part.Get("text").String()); text != "" {
				textParts = append(textParts, text)
			}
			return true
		})
		return strings.Join(textParts, " ")
	}
	return ""
}

func upstreamHealthProbeCachedTokens(body []byte) int64 {
	for _, path := range []string{
		"usage.prompt_tokens_details.cached_tokens",
		"usage.input_tokens_details.cached_tokens",
		"usage.cached_tokens",
		"usage.total_cached_tokens",
	} {
		if value := gjson.GetBytes(body, path); value.Exists() && value.Int() > 0 {
			return value.Int()
		}
	}
	return 0
}

func (h *Handler) probeUpstreamBillingEntry(ctx context.Context, auth *coreauth.Auth) (upstreamBillingProbeEntry, bool) {
	if h == nil || auth == nil {
		return upstreamBillingProbeEntry{}, false
	}
	if !isEligibleUpstreamBillingAuth(auth) {
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

	reqURL, err := buildUpstreamBillingProbeURL(baseURL)
	if err != nil {
		entry.Status = "failed"
		entry.Error = "request-build-failed"
		return entry, true
	}
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

func buildUpstreamBillingProbeURL(baseURL string) (string, error) {
	trimmed := strings.TrimSpace(baseURL)
	if trimmed == "" {
		return "", fmt.Errorf("empty base url")
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", err
	}
	parsed.Fragment = ""
	parsed.RawQuery = ""

	path := strings.TrimRight(parsed.Path, "/")
	switch {
	case path == "":
		path = "/v1/sub2api/billing"
	case strings.HasSuffix(path, "/sub2api/billing"):
		// keep the configured endpoint path as-is.
	case strings.HasSuffix(path, "/v1"):
		path += "/sub2api/billing"
	default:
		path += "/v1/sub2api/billing"
	}
	parsed.Path = path
	return parsed.String(), nil
}

func buildOpenAICompatibilityChatCompletionsURL(baseURL string) (string, error) {
	trimmed := strings.TrimSpace(baseURL)
	if trimmed == "" {
		return "", fmt.Errorf("empty base url")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", err
	}
	parsed.Fragment = ""
	parsed.RawQuery = ""
	path := strings.TrimRight(parsed.Path, "/")
	switch {
	case path == "":
		path = "/chat/completions"
	case strings.HasSuffix(path, "/chat/completions"):
	case strings.HasSuffix(path, "/v1"):
		path += "/chat/completions"
	default:
		path += "/chat/completions"
	}
	parsed.Path = path
	return parsed.String(), nil
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

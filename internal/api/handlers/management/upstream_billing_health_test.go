package management

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestBuildOpenAICompatibilityChatCompletionsURLFollowsBaseURLVersion(t *testing.T) {
	tests := []struct {
		name string
		base string
		want string
	}{
		{name: "root", base: "https://example.test", want: "https://example.test/chat/completions"},
		{name: "root trailing slash", base: "https://example.test/", want: "https://example.test/chat/completions"},
		{name: "v1", base: "https://example.test/v1", want: "https://example.test/v1/chat/completions"},
		{name: "v1 trailing slash", base: "https://example.test/v1/", want: "https://example.test/v1/chat/completions"},
		{name: "api prefix", base: "https://example.test/api", want: "https://example.test/api/chat/completions"},
		{name: "api v1 prefix", base: "https://example.test/api/v1", want: "https://example.test/api/v1/chat/completions"},
		{name: "complete endpoint", base: "https://example.test/api/chat/completions?x=1", want: "https://example.test/api/chat/completions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildOpenAICompatibilityChatCompletionsURL(tt.base)
			if err != nil {
				t.Fatalf("build URL: %v", err)
			}
			if got != tt.want {
				t.Fatalf("URL = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildUpstreamHealthProbeURLByProvider(t *testing.T) {
	tests := []struct {
		provider, base, model, wantPath string
	}{
		{"openai-compatibility", "https://example.test/v1", "gpt", "/v1/chat/completions"},
		{"codex", "https://example.test/v1", "gpt-5.5", "/v1/responses"},
		{"xai", "https://example.test/v1", "grok-4", "/v1/responses"},
		{"claude", "https://example.test", "claude-sonnet", "/v1/messages"},
		{"gemini", "https://example.test", "gemini-2.5-flash", "/v1beta/models/gemini-2.5-flash:generateContent"},
		{"gemini-interactions", "https://example.test/v1beta", "agent", "/v1beta/interactions"},
		{"claude", "", "claude-sonnet", "/v1/messages"},
	}
	for _, tt := range tests {
		t.Run(tt.provider, func(t *testing.T) {
			auth := &coreauth.Auth{Provider: tt.provider}
			got, _ := buildUpstreamHealthProbeURL(auth, tt.base, tt.model)
			parsed, err := url.Parse(got)
			if err != nil || parsed.Path != tt.wantPath {
				t.Fatalf("URL=%q path=%q want %q", got, parsed.Path, tt.wantPath)
			}
		})
	}
}

func TestDefaultBaseURLAuthIsHealthEligibleButNotBillingEligible(t *testing.T) {
	auth := &coreauth.Auth{
		Provider:   "claude",
		Attributes: map[string]string{"api_key": "test-key"},
	}
	if !isEligibleUpstreamProbeAuth(auth) {
		t.Fatal("default-base API key auth should be eligible for health probes")
	}
	if isEligibleUpstreamBillingAuth(auth) {
		t.Fatal("Sub2API billing probe requires an explicit base URL")
	}
}

func TestUpstreamProbeFingerprintTracksModelsAndHeaders(t *testing.T) {
	base := &coreauth.Auth{ID: "id", Index: "1", Provider: "claude", Attributes: map[string]string{"api_key": "key", "models_hash": "a", "header:X-Test": "one"}}
	changedModel := base.Clone()
	changedModel.Attributes["models_hash"] = "b"
	changedHeader := base.Clone()
	changedHeader.Attributes["header:X-Test"] = "two"
	first := upstreamProbeFingerprint(base, upstreamProbeHealth, "model")
	if first == upstreamProbeFingerprint(changedModel, upstreamProbeHealth, "model") {
		t.Fatal("models_hash change must refresh health probe tasks")
	}
	if first == upstreamProbeFingerprint(changedHeader, upstreamProbeHealth, "model") {
		t.Fatal("custom header change must refresh health probe tasks")
	}
}

func TestSuccessfulUpstreamHealthProbeResponseByProtocol(t *testing.T) {
	tests := []struct {
		protocol, body string
	}{
		{"responses", "{\"output_text\":\"answer: 46\"}"},
		{"xai-responses", "{\"output\":[{\"content\":[{\"text\":\"46\"}]}]}"},
		{"claude", "{\"content\":[{\"type\":\"text\",\"text\":\"46\"}]}"},
		{"gemini", "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"46\"}]}}]}"},
		{"interactions", "{\"steps\":[{\"content\":[{\"text\":\"46\"}]}]}"},
	}
	for _, tt := range tests {
		if !isSuccessfulUpstreamHealthProbeResponseForProtocol([]byte(tt.body), "46", tt.protocol) {
			t.Fatalf("protocol %s was not recognized", tt.protocol)
		}
	}
}

func TestUpstreamHealthProbePayloadStaysPlainChatCompletions(t *testing.T) {
	challenge := upstreamHealthProbeChallenge{
		Prompt:   "17 + 29 = ?",
		Expected: "46",
		CacheKey: "cliproxy-health-test-session",
	}
	payload := upstreamHealthProbePayload("gpt-5.5", challenge)

	if got := payload["model"]; got != "gpt-5.5" {
		t.Fatalf("model = %#v, want gpt-5.5", got)
	}
	if got := payload["max_tokens"]; got != upstreamHealthProbeMaxTokens {
		t.Fatalf("max_tokens = %#v, want %d", got, upstreamHealthProbeMaxTokens)
	}
	if got := payload["stream"]; got != false {
		t.Fatalf("stream = %#v, want false", got)
	}
	if got := payload["instructions"]; got != upstreamHealthProbeInstructions {
		t.Fatalf("instructions = %#v, want %q", got, upstreamHealthProbeInstructions)
	}
	if got := payload["prompt_cache_key"]; got != challenge.CacheKey {
		t.Fatalf("prompt_cache_key = %#v, want %q", got, challenge.CacheKey)
	}
	if _, ok := payload["reasoning_effort"]; ok {
		t.Fatal("health probe must not send reasoning_effort")
	}
	if _, ok := payload["temperature"]; ok {
		t.Fatal("health probe must not send temperature")
	}

	messages, ok := payload["messages"].([]map[string]string)
	if !ok || len(messages) != 1 || messages[0]["role"] != "user" || messages[0]["content"] != challenge.Prompt {
		t.Fatalf("unexpected messages payload: %#v", payload["messages"])
	}
}

func TestNewUpstreamHealthProbeChallenge(t *testing.T) {
	challenge := newUpstreamHealthProbeChallenge()
	if challenge.Prompt == "" || challenge.Expected == "" {
		t.Fatalf("empty challenge: %#v", challenge)
	}
	if challenge.CacheKey == "" {
		t.Fatal("health probe must use an isolated cache key")
	}
}

func TestPutUpstreamBillingProbeStoresManualRateAsSuccessfulProbe(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	auth, err := manager.Register(context.Background(), testUpstreamProbeAuth("https://example.test/v1"))
	if err != nil {
		t.Fatalf("register test auth: %v", err)
	}
	authIndex := auth.EnsureIndex()
	checkedAt := time.Now().UTC().Add(-time.Minute)
	h := &Handler{
		authManager: manager,
		upstreamBillingProbeCache: &upstreamBillingProbeCache{entries: []upstreamBillingProbeEntry{{
			AuthIndex: authIndex,
			Status:    "failed",
			HealthHistory: []upstreamHealthProbeSample{{
				Status:    "ok",
				LatencyMS: 25,
				CheckedAt: checkedAt,
			}},
		}}},
	}
	body := []byte(fmt.Sprintf(`{"auth-index":%q,"effective-rate-multiplier":1.25}`, authIndex))
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPut, "/upstream-billing-probe", bytes.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")

	h.PutUpstreamBillingProbe(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var got upstreamBillingProbeEntry
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Status != "ok" || got.Error != "" {
		t.Fatalf("status = %q error = %q, want successful probe", got.Status, got.Error)
	}
	for name, value := range map[string]*float64{
		"group":     got.GroupRateMultiplier,
		"resolved":  got.ResolvedRateMultiplier,
		"effective": got.EffectiveRateMultiplier,
	} {
		if value == nil || *value != 1.25 {
			t.Fatalf("%s multiplier = %v, want 1.25", name, value)
		}
	}
	if len(got.HealthHistory) != 1 || !got.HealthHistory[0].CheckedAt.Equal(checkedAt) {
		t.Fatalf("health history was not preserved: %#v", got.HealthHistory)
	}
}

func TestSuccessfulUpstreamHealthProbeResponse(t *testing.T) {
	for _, body := range [][]byte{
		[]byte(`{"choices":[{"message":{"content":"46"}}]}`),
		[]byte(`{"choices":[{"message":{"content":"The answer is 46."}}]}`),
		[]byte(`{"choices":[{"message":{"content":[{"type":"text","text":"46"}]}}]}`),
	} {
		if !isSuccessfulUpstreamHealthProbeResponse(body, "46") {
			t.Fatalf("expected a successful health response for %s", body)
		}
	}

	for _, body := range [][]byte{
		[]byte(`{"choices":[]}`),
		[]byte(`{"choices":[{"message":{"content":"45"}}]}`),
		[]byte(`{"choices":[{"message":{"content":null}}]}`),
		[]byte(`not json`),
	} {
		if isSuccessfulUpstreamHealthProbeResponse(body, "46") {
			t.Fatalf("expected a failed health response for %s", body)
		}
	}
}

func TestUpstreamHealthProbeCachedTokens(t *testing.T) {
	for _, body := range [][]byte{
		[]byte(`{"usage":{"prompt_tokens_details":{"cached_tokens":3840}}}`),
		[]byte(`{"usage":{"input_tokens_details":{"cached_tokens":3840}}}`),
		[]byte(`{"usage":{"cached_tokens":3840}}`),
		[]byte(`{"usage":{"total_cached_tokens":3840}}`),
	} {
		if got := upstreamHealthProbeCachedTokens(body); got != 3840 {
			t.Fatalf("cached tokens = %d, want 3840 for %s", got, body)
		}
	}

	for _, body := range [][]byte{
		[]byte(`{"usage":{"prompt_tokens_details":{"cached_tokens":0}}}`),
		[]byte(`{"choices":[{"message":{"content":"OK"}}]}`),
		[]byte(`not json`),
	} {
		if got := upstreamHealthProbeCachedTokens(body); got != 0 {
			t.Fatalf("cached tokens = %d, want 0 for %s", got, body)
		}
	}
}

func TestCachedTokensAreMetadataForSuccessfulHealthResponse(t *testing.T) {
	body := []byte(`{"choices":[{"message":{"content":"46"}}],"usage":{"prompt_tokens_details":{"cached_tokens":3840}}}`)
	if !isSuccessfulUpstreamHealthProbeResponse(body, "46") {
		t.Fatal("a valid model response must stay healthy when cached token metadata is present")
	}
	if got := upstreamHealthProbeCachedTokens(body); got != 3840 {
		t.Fatalf("cached tokens = %d, want 3840", got)
	}
}

func TestDispatchUpstreamProbesSkipsDuplicateTasksWithoutWaiting(t *testing.T) {
	billingStarted := make(chan struct{}, 1)
	healthStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	var billingCalls atomic.Int32
	var healthCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sub2api/billing":
			billingCalls.Add(1)
			billingStarted <- struct{}{}
			<-release
			writeTestBillingResponse(w)
		case "/chat/completions":
			healthCalls.Add(1)
			healthStarted <- struct{}{}
			<-release
			writeTestHealthResponse(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	h := newUpstreamProbeTestHandler(t, server.URL, true)
	startedAt := time.Now()
	first := h.dispatchUpstreamProbes()
	if elapsed := time.Since(startedAt); elapsed > 200*time.Millisecond {
		t.Fatalf("dispatch waited for upstream work: %s", elapsed)
	}
	if len(first.Accepted) != 2 {
		t.Fatalf("first accepted = %d, want 2", len(first.Accepted))
	}
	waitTestSignal(t, billingStarted)
	waitTestSignal(t, healthStarted)

	second := h.dispatchUpstreamProbes()
	if len(second.Accepted) != 0 || len(second.Skipped) != 2 {
		t.Fatalf("duplicate dispatch accepted=%d skipped=%d, want 0/2", len(second.Accepted), len(second.Skipped))
	}
	close(release)
	waitForProbeTasks(t, h, 0, 2*time.Second)
	if billingCalls.Load() != 1 || healthCalls.Load() != 1 {
		t.Fatalf("calls billing=%d health=%d, want 1/1", billingCalls.Load(), healthCalls.Load())
	}
}

func TestSlowHealthProbeHasNoOverallResponseTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			http.NotFound(w, r)
			return
		}
		time.Sleep(upstreamBillingProbeTimeout + 250*time.Millisecond)
		writeTestHealthResponse(w, r)
	}))
	defer server.Close()

	auth := testUpstreamProbeAuth(server.URL)
	startedAt := time.Now()
	sample, ok := (&Handler{}).probeUpstreamHealth(context.Background(), auth, "gpt-5.5")
	if !ok || sample.Status != "ok" {
		t.Fatalf("slow health result ok=%v sample=%#v", ok, sample)
	}
	if elapsed := time.Since(startedAt); elapsed < upstreamBillingProbeTimeout {
		t.Fatalf("server did not exercise the old timeout boundary: %s", elapsed)
	}
}

func TestBlockedUpstreamDoesNotDelayAnotherUpstreamResult(t *testing.T) {
	blockedHealthStarted := make(chan struct{}, 1)
	releaseBlockedHealth := make(chan struct{})
	blockedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sub2api/billing":
			writeTestBillingResponse(w)
		case "/chat/completions":
			blockedHealthStarted <- struct{}{}
			<-releaseBlockedHealth
			writeTestHealthResponse(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer blockedServer.Close()
	fastServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sub2api/billing":
			writeTestBillingResponse(w)
		case "/chat/completions":
			writeTestHealthResponse(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer fastServer.Close()

	manager := coreauth.NewManager(nil, nil, nil)
	blockedAuth := testUpstreamProbeAuthWithID("blocked", "blocked-key", blockedServer.URL)
	fastAuth := testUpstreamProbeAuthWithID("fast", "fast-key", fastServer.URL)
	for _, auth := range []*coreauth.Auth{blockedAuth, fastAuth} {
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("register %s: %v", auth.ID, err)
		}
	}
	h := &Handler{
		cfg: &config.Config{UpstreamBillingProbe: config.UpstreamBillingProbe{
			IntervalMinutes: 30,
			HealthEnabled:   true,
			HealthModel:     "gpt-5.5",
		}},
		authManager:           manager,
		upstreamProbeInFlight: make(map[upstreamProbeTaskKey]upstreamProbeTask),
	}
	h.dispatchUpstreamProbes()
	waitTestSignal(t, blockedHealthStarted)

	fastIndex := fastAuth.EnsureIndex()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entry, ok := h.previousUpstreamBillingProbeEntry(fastIndex)
		if ok && len(entry.HealthHistory) == 1 && entry.HealthHistory[0].Status == "ok" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	entry, ok := h.previousUpstreamBillingProbeEntry(fastIndex)
	if !ok || len(entry.HealthHistory) != 1 || entry.HealthHistory[0].Status != "ok" {
		t.Fatalf("fast upstream did not commit independently: %#v", entry)
	}

	blockedIndex := blockedAuth.EnsureIndex()
	blockedStillRunning := false
	for _, task := range h.upstreamProbeRunningSnapshot() {
		if task.AuthIndex == blockedIndex && task.Kind == upstreamProbeHealth {
			blockedStillRunning = true
			break
		}
	}
	if !blockedStillRunning {
		t.Fatal("blocked health task unexpectedly stopped before release")
	}
	close(releaseBlockedHealth)
	waitForProbeTasks(t, h, 0, 2*time.Second)
}

func newUpstreamProbeTestHandler(t *testing.T, baseURL string, healthEnabled bool) *Handler {
	t.Helper()
	manager := coreauth.NewManager(nil, nil, nil)
	if _, err := manager.Register(context.Background(), testUpstreamProbeAuth(baseURL)); err != nil {
		t.Fatalf("register test auth: %v", err)
	}
	return &Handler{
		cfg: &config.Config{UpstreamBillingProbe: config.UpstreamBillingProbe{
			IntervalMinutes: 30,
			HealthEnabled:   healthEnabled,
			HealthModel:     "gpt-5.5",
		}},
		authManager:           manager,
		upstreamProbeInFlight: make(map[upstreamProbeTaskKey]upstreamProbeTask),
	}
}

func testUpstreamProbeAuth(baseURL string) *coreauth.Auth {
	return testUpstreamProbeAuthWithID("test-openai-compatibility", "test-key", baseURL)
}

func testUpstreamProbeAuthWithID(id, apiKey, baseURL string) *coreauth.Auth {
	return &coreauth.Auth{
		ID:       id,
		Provider: "openai-compatibility",
		Label:    id,
		Attributes: map[string]string{
			"api_key":     apiKey,
			"base_url":    baseURL,
			"compat_name": id,
		},
	}
}

func writeTestBillingResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"object":"sub2api.key_billing","schema_version":1,"billing_scope":"token","group_rate_multiplier":1,"resolved_rate_multiplier":1,"peak_rate_enabled":false,"effective_rate_multiplier":1}`))
}

func writeTestHealthResponse(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
	}
	_ = json.NewDecoder(r.Body).Decode(&request)
	left, right := 0, 0
	if len(request.Messages) > 0 {
		_, _ = fmt.Sscanf(request.Messages[0].Content, "%d + %d = ?", &left, &right)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"content":"%d"}}]}`, left+right)
}

func waitTestSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for upstream request")
	}
}

func waitForProbeTasks(t *testing.T, h *Handler, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(h.upstreamProbeRunningSnapshot()) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("running tasks = %d, want %d", len(h.upstreamProbeRunningSnapshot()), want)
}

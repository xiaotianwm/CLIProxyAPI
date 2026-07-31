package management

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

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
		case "/v1/chat/completions":
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
		if r.URL.Path != "/v1/chat/completions" {
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
		case "/v1/chat/completions":
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
		case "/v1/chat/completions":
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

package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestNormalizeCompactionSummaryOutputPreservesEncryptedContent(t *testing.T) {
	body := []byte(`{"output":[{"id":"msg_1","type":"message"},{"id":"cmp_1","type":"compaction_summary","encrypted_content":"opaque-state"}]}`)
	normalized := normalizeCompactionSummaryOutput(body)
	if got := gjson.GetBytes(normalized, "output.1.type").String(); got != "compaction" {
		t.Fatalf("type = %q, want compaction; body=%s", got, normalized)
	}
	if got := gjson.GetBytes(normalized, "output.1.encrypted_content").String(); got != "opaque-state" {
		t.Fatalf("encrypted_content = %q, want opaque-state; body=%s", got, normalized)
	}
	if got := gjson.GetBytes(normalized, "output.0.type").String(); got != "message" {
		t.Fatalf("history item type = %q, want message; body=%s", got, normalized)
	}
}

func TestCodexExecutorCompactAddsDefaultInstructionsWithoutInjectingImageTool(t *testing.T) {
	cases := []struct {
		name    string
		payload string
	}{
		{
			name:    "missing instructions",
			payload: `{"model":"gpt-5.4","input":[{"type":"message","role":"user","content":"history"},{"type":"compaction_trigger"}]}`,
		},
		{
			name:    "null instructions",
			payload: `{"model":"gpt-5.4","instructions":null,"input":[{"type":"message","role":"user","content":"history"},{"type":"compaction_trigger"}]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath string
			var gotBody []byte
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				body, _ := io.ReadAll(r.Body)
				gotBody = body
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"resp_1","object":"response.compaction","usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}`))
			}))
			defer server.Close()

			executor := NewCodexExecutor(&config.Config{})
			auth := &cliproxyauth.Auth{Attributes: map[string]string{
				"base_url": server.URL,
				"api_key":  "test",
			}}

			resp, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
				Model:   "gpt-5.4",
				Payload: []byte(tc.payload),
			}, cliproxyexecutor.Options{
				SourceFormat: sdktranslator.FromString("openai-response"),
				Alt:          "responses/compact",
				Stream:       false,
			})
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}
			if gotPath != "/responses/compact" {
				t.Fatalf("path = %q, want %q", gotPath, "/responses/compact")
			}
			if instructions := gjson.GetBytes(gotBody, "instructions"); instructions.Type != gjson.String || instructions.String() != "" {
				t.Fatalf("instructions = %s, want empty string; body=%s", instructions.Raw, gotBody)
			}
			if gjson.GetBytes(gotBody, "tools").Exists() {
				t.Fatalf("compact request injected image_generation tool: %s", gotBody)
			}
			input := gjson.GetBytes(gotBody, "input").Array()
			if len(input) != 1 || input[0].Get("type").String() != "message" {
				t.Fatalf("compact history changed or trigger was retained: %s", gotBody)
			}
			if string(resp.Payload) != `{"id":"resp_1","object":"response.compaction","usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}` {
				t.Fatalf("payload = %s", string(resp.Payload))
			}
		})
	}
}

func TestCodexExecutorCompactionTriggerStreamUsesCompactEndpoint(t *testing.T) {
	var gotPath string
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		gotBody = body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_compact_1","object":"response.compaction","model":"gpt-5.4","output":[{"id":"msg_1","type":"message","role":"user","content":[]},{"id":"cmp_1","type":"compaction_summary","encrypted_content":"opaque-state"}],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}`))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "test",
	}}

	stream, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4",
		Payload: []byte(`{"model":"gpt-5.4","stream":true,"input":[{"type":"message","role":"user","content":"history"},{"type":"compaction_trigger"}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai-response"),
		Stream:       true,
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}
	if gotPath != "/responses/compact" {
		t.Fatalf("path = %q, want /responses/compact", gotPath)
	}
	if xaiInputHasItemType(gotBody, "compaction_trigger") {
		t.Fatalf("compaction_trigger reached compact endpoint: %s", gotBody)
	}

	var out bytes.Buffer
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		out.Write(chunk.Payload)
	}
	body := out.String()
	if !bytes.Contains([]byte(body), []byte(`"type":"response.output_item.done"`)) ||
		!bytes.Contains([]byte(body), []byte(`"type":"compaction"`)) ||
		!bytes.Contains([]byte(body), []byte(`"encrypted_content":"opaque-state"`)) {
		t.Fatalf("stream did not expose compaction item: %s", body)
	}
	if bytes.Contains([]byte(body), []byte(`"id":"msg_1"`)) {
		t.Fatalf("stream exposed non-compaction history item: %s", body)
	}
	if bytes.Contains([]byte(body), []byte(`"type":"compaction_summary"`)) {
		t.Fatalf("stream exposed unnormalized compaction_summary: %s", body)
	}
}

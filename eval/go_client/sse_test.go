package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseSSEStreamDetectsChatFirstToken(t *testing.T) {
	body := strings.NewReader(
		"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n" +
			"data: [DONE]\n\n",
	)

	result := parseSSEStream(body, time.Now().Add(-5*time.Millisecond))
	if result.FirstTokenAt.IsZero() {
		t.Fatal("expected first token timestamp for chat SSE stream")
	}
	if result.TTFT <= 0 {
		t.Fatalf("expected positive TTFT, got %v", result.TTFT)
	}
}

func TestParseSSEStreamDetectsCompletionFirstToken(t *testing.T) {
	body := strings.NewReader(
		"data: {\"choices\":[{\"text\":\"hello\"}]}\n\n" +
			"data: [DONE]\n\n",
	)

	result := parseSSEStream(body, time.Now().Add(-5*time.Millisecond))
	if result.FirstTokenAt.IsZero() {
		t.Fatal("expected first token timestamp for completion SSE stream")
	}
	if result.TTFT <= 0 {
		t.Fatalf("expected positive TTFT, got %v", result.TTFT)
	}
}

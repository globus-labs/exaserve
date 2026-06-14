package main

import (
	"io"
	"strings"
	"testing"
	"time"
)

// TestParseSSEStreamBodyClosedMidStream simulates the stall watchdog closing a
// wedged stream's body: parseSSEStream must surface an error (so the request is
// recorded as a failure, classified "stall" by the caller) while keeping the
// chunks seen before the close.
func TestParseSSEStreamBodyClosedMidStream(t *testing.T) {
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
		time.Sleep(20 * time.Millisecond)
		// Watchdog-equivalent: close the body while parseSSEStream blocks on the
		// next read (the stream went silent).
		_ = pw.CloseWithError(io.ErrUnexpectedEOF)
	}()
	result := parseSSEStream(pr, time.Now(), nil)
	if result.Err == nil {
		t.Fatal("expected an error when the body is closed mid-stream")
	}
	if result.NumChunks != 1 {
		t.Fatalf("expected the 1 pre-close chunk to be captured, got %d", result.NumChunks)
	}
}

func TestParseSSEStreamDetectsChatFirstToken(t *testing.T) {
	body := strings.NewReader(
		"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n" +
			"data: [DONE]\n\n",
	)

	result := parseSSEStream(body, time.Now().Add(-5*time.Millisecond), nil)
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

	result := parseSSEStream(body, time.Now().Add(-5*time.Millisecond), nil)
	if result.FirstTokenAt.IsZero() {
		t.Fatal("expected first token timestamp for completion SSE stream")
	}
	if result.TTFT <= 0 {
		t.Fatalf("expected positive TTFT, got %v", result.TTFT)
	}
}

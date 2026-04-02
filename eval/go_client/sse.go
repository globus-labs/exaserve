package main

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"
	"time"
)

// sseResult holds the aggregated outcome of parsing an SSE stream.
type sseResult struct {
	FirstTokenAt time.Time
	TTFT         time.Duration
	Usage        *usageBlock
	Err          error
}

// sseChoice mirrors the choices[].delta structure in an SSE chunk.
type sseChoice struct {
	Delta struct {
		Content string `json:"content"`
	} `json:"delta"`
	Text string `json:"text"`
}

// sseChunk mirrors the data payload of one SSE event.
type sseChunk struct {
	Choices []sseChoice `json:"choices"`
	Usage   *usageBlock `json:"usage"`
}

// parseSSEStream reads an OpenAI-compatible SSE stream from body.
// It records the timestamp of the first token (first chunk with non-empty content)
// and extracts usage from the final chunk. requestStart is used to compute TTFT.
func parseSSEStream(body io.Reader, requestStart time.Time) sseResult {
	scanner := bufio.NewScanner(body)
	// Increase buffer for potentially large SSE lines.
	scanner.Buffer(make([]byte, 0, 64*1024), 256*1024)

	var result sseResult
	gotFirstToken := false

	for scanner.Scan() {
		line := scanner.Text()

		// SSE format: "data: <json>" or "data: [DONE]"
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}

		var chunk sseChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue // skip malformed chunks
		}

		// Record first token time: first chunk with non-empty content delta.
		if !gotFirstToken {
			for _, c := range chunk.Choices {
				if c.Delta.Content != "" || c.Text != "" {
					result.FirstTokenAt = time.Now()
					result.TTFT = result.FirstTokenAt.Sub(requestStart)
					gotFirstToken = true
					break
				}
			}
		}

		// vLLM includes usage in the last data chunk (before [DONE]).
		if chunk.Usage != nil {
			result.Usage = chunk.Usage
		}
	}

	if err := scanner.Err(); err != nil {
		result.Err = err
	}
	return result
}

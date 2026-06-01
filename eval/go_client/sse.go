package main

import (
	"bufio"
	"encoding/json"
	"io"
	"sort"
	"strings"
	"time"
)

// sseResult holds the aggregated outcome of parsing an SSE stream.
type sseResult struct {
	FirstTokenAt time.Time
	TTFT         time.Duration
	// Per-request inter-token latency (Time-Between-Tokens) summary, computed
	// from the gaps between consecutive content-bearing chunks. These are the
	// percentiles the paper's SLO is defined against (P99 TBT), as opposed to a
	// single mean TPOT. Zero-valued when fewer than two tokens were observed.
	TBTp50    time.Duration
	TBTp99    time.Duration
	TBTMax    time.Duration
	NumChunks int // content-bearing chunks observed (≈ decoded tokens)
	Usage     *usageBlock
	Err       error
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

// percentileIndex returns the nearest-rank index into a length-n sorted slice
// for quantile q in [0,1]. Matches the int(len*q) convention used elsewhere in
// the eval code (see eval/plot/sat_finder.py) but clamps the upper bound.
func percentileIndex(n int, q float64) int {
	if n <= 1 {
		return 0
	}
	idx := int(float64(n) * q)
	if idx >= n {
		idx = n - 1
	}
	if idx < 0 {
		idx = 0
	}
	return idx
}

// parseSSEStream reads an OpenAI-compatible SSE stream from body. It records the
// timestamp of the first token (first chunk with non-empty content), the gaps
// between every subsequent content chunk (summarised as P50/P99/max TBT), and
// extracts usage from the final chunk. requestStart is used to compute TTFT.
func parseSSEStream(body io.Reader, requestStart time.Time) sseResult {
	scanner := bufio.NewScanner(body)
	// Increase buffer for potentially large SSE lines.
	scanner.Buffer(make([]byte, 0, 64*1024), 256*1024)

	var result sseResult
	gotFirstToken := false
	var lastTokenAt time.Time
	// Inter-token gaps; for k content chunks there are k-1 gaps.
	var gaps []time.Duration

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

		// A chunk "carries a token" if any choice has non-empty content/text.
		hasContent := false
		for _, c := range chunk.Choices {
			if c.Delta.Content != "" || c.Text != "" {
				hasContent = true
				break
			}
		}
		if hasContent {
			now := time.Now()
			result.NumChunks++
			if !gotFirstToken {
				result.FirstTokenAt = now
				result.TTFT = now.Sub(requestStart)
				gotFirstToken = true
			} else {
				gaps = append(gaps, now.Sub(lastTokenAt))
			}
			lastTokenAt = now
		}

		// vLLM includes usage in the last data chunk (before [DONE]).
		if chunk.Usage != nil {
			result.Usage = chunk.Usage
		}
	}

	if len(gaps) > 0 {
		sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
		result.TBTp50 = gaps[percentileIndex(len(gaps), 0.50)]
		result.TBTp99 = gaps[percentileIndex(len(gaps), 0.99)]
		result.TBTMax = gaps[len(gaps)-1]
	}

	if err := scanner.Err(); err != nil {
		result.Err = err
	}
	return result
}

package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSummaryCompletedCountIncludesFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.jsonl")
	rows := [][]resultRecord{{
		{ReqID: "success", Success: true, Latency: 0.1},
		{ReqID: "failure", Success: false, Error: "timeout", ErrorClass: "timeout"},
	}}
	if err := writeResults(path, true, rows, 0, 0, 0, 1, "test", nil); err != nil {
		t.Fatalf("writeResults: %v", err)
	}

	handle, err := os.Open(path)
	if err != nil {
		t.Fatalf("open results: %v", err)
	}
	defer handle.Close()
	scanner := bufio.NewScanner(handle)
	if !scanner.Scan() {
		t.Fatalf("missing summary: %v", scanner.Err())
	}
	var summary summaryRecord
	if err := json.Unmarshal(scanner.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.RequestsScheduled != 2 || summary.RequestsCompleted != 2 || summary.Errors != 1 {
		t.Fatalf("unexpected summary counts: %+v", summary)
	}
	if summary.LatencyHistogram.Count != 1 {
		t.Fatalf("summary latency histogram does not cover the successful request: %+v", summary)
	}
	if summary.LatencyQuantileMethod != latencyQuantileMethod {
		t.Fatalf("summary omitted the histogram estimator identity: %+v", summary)
	}
	if summary.P50S != PercentileFromHistogram(&summary.LatencyHistogram, 0.50) ||
		summary.P99S != PercentileFromHistogram(&summary.LatencyHistogram, 0.99) {
		t.Fatalf("summary quantiles do not use their published histogram: %+v", summary)
	}
	if scanner.Scan() {
		t.Fatalf("unexpected row after summary: %s", scanner.Text())
	}
}

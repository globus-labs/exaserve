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
	if scanner.Scan() {
		t.Fatalf("unexpected row after summary: %s", scanner.Text())
	}
}

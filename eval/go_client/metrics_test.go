package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestResolveMaxActiveRequests(t *testing.T) {
	value, err := resolveMaxActiveRequests(8, 0)
	if err != nil {
		t.Fatalf("resolveMaxActiveRequests returned error: %v", err)
	}
	if value != 8 {
		t.Fatalf("expected 8, got %d", value)
	}

	value, err = resolveMaxActiveRequests(0, 4)
	if err != nil {
		t.Fatalf("resolveMaxActiveRequests legacy alias returned error: %v", err)
	}
	if value != 4 {
		t.Fatalf("expected 4 from legacy alias, got %d", value)
	}

	if _, err := resolveMaxActiveRequests(8, 4); err == nil {
		t.Fatal("expected disagreement between max-active-requests and legacy concurrency to fail")
	}
}

func TestDurationHistogramSnapshot(t *testing.T) {
	hist := newDurationHistogram([]time.Duration{time.Millisecond, 10 * time.Millisecond})
	hist.Observe(500 * time.Microsecond)
	hist.Observe(5 * time.Millisecond)
	hist.Observe(20 * time.Millisecond)

	snapshot := hist.Snapshot()
	if snapshot.Count != 3 {
		t.Fatalf("expected 3 observations, got %d", snapshot.Count)
	}
	if len(snapshot.Counts) != 3 {
		t.Fatalf("expected 3 histogram buckets, got %d", len(snapshot.Counts))
	}
	if snapshot.Counts[0] != 1 || snapshot.Counts[1] != 1 || snapshot.Counts[2] != 1 {
		t.Fatalf("unexpected histogram distribution: %#v", snapshot.Counts)
	}
}

func TestNewHTTPClientHonorsMaxConnsPerHost(t *testing.T) {
	var active atomic.Int64
	var maxSeen atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value := active.Add(1)
		recordMax(&maxSeen, value)
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
		active.Add(-1)
	}))
	defer server.Close()

	client := newHTTPClient(1, 1, 10)
	var wg sync.WaitGroup
	for idx := 0; idx < 4; idx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Post(server.URL, "application/json", strings.NewReader("{}"))
			if err != nil {
				t.Errorf("client.Post failed: %v", err)
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
	}
	wg.Wait()

	if maxSeen.Load() > 1 {
		t.Fatalf("expected max concurrent handler count <= 1, got %d", maxSeen.Load())
	}
}

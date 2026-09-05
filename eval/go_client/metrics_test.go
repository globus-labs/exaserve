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

func TestDefaultHistogramCoversTwoHoursWithDenseRelativeBuckets(t *testing.T) {
	if len(defaultHistogramBounds) < 100 || defaultHistogramBounds[len(defaultHistogramBounds)-1] != 7200*time.Second {
		t.Fatalf("unexpected histogram coverage: count=%d last=%s", len(defaultHistogramBounds), defaultHistogramBounds[len(defaultHistogramBounds)-1])
	}
	for index := 1; index < len(defaultHistogramBounds); index++ {
		ratio := float64(defaultHistogramBounds[index]) / float64(defaultHistogramBounds[index-1])
		if ratio > 1.021 {
			t.Fatalf("histogram gap %d is too wide: %.6f", index, ratio)
		}
	}
}

func TestOverflowPercentileUsesConservativeFiniteBoundary(t *testing.T) {
	snapshot := histogramSnapshot{
		BucketUpperBoundsS: []float64{60, 7200, -1},
		Counts:             []uint64{98, 0, 2},
		Count:              100,
		SumS:               6100,
	}
	if observed := PercentileFromHistogram(&snapshot, 0.99); observed != 7200 {
		t.Fatalf("overflow p99 must use finite lower bound, got %f", observed)
	}
}

func TestHistogramPercentileUsesCeilingNearestRank(t *testing.T) {
	snapshot := histogramSnapshot{
		BucketUpperBoundsS: []float64{1, 2, -1},
		Counts:             []uint64{1, 1, 1},
		Count:              3,
		SumS:               6,
	}
	if observed := PercentileFromHistogram(&snapshot, 0.50); observed != 2 {
		t.Fatalf("three-sample p50 must select rank two, got %f", observed)
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

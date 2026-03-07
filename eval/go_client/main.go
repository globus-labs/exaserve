// go_dispatch: high-throughput HTTP request dispatcher for replay_client.py.
//
// Reads a trace partition JSONL file, dispatches requests on schedule using
// goroutines over a persistent keepalive connection pool (HTTP/1.1), and
// writes per-request results to a JSONL result file.
//
// Port exhaustion is prevented by bounding the connection pool size: each
// goroutine acquires a semaphore slot before sending, so the number of
// concurrent in-flight requests — and therefore open TCP connections — is
// capped at --concurrency. This is equivalent to the Python worker pool
// size in replay_client.py.
//
// Usage:
//
//	./go_dispatch \
//	  --base-urls "http://0.0.0.0:8000" \
//	  --run-t0 1709744400.123456 \
//	  --trace-file /tmp/rank0_trace.jsonl \
//	  --result-file /tmp/rank0_results.jsonl \
//	  [--generation-mode deterministic] \
//	  [--include-tp] \
//	  [--timeout 3600] \
//	  [--concurrency 2000]
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// Input: trace request
// ---------------------------------------------------------------------------

type traceRequest struct {
	Timestamp        float64 `json:"timestamp"`
	Model            string  `json:"model"`
	Mode             string  `json:"mode"`
	Prompt           string  `json:"prompt"`
	InputLen         int     `json:"input_len"`
	OutputLen        int     `json:"output_len"`
	TensorParallelSz int     `json:"tensor_parallel_size"`
	ReqID            string  `json:"req_id"`
}

// preparedRequest pairs a trace entry with its pre-built JSON body so that
// json.Marshal is never called on the hot path inside goroutines.
type preparedRequest struct {
	traceRequest
	body     []byte // pre-built JSON payload
	endpoint string // "/v1/chat/completions" or "/v1/completions"
}

// ---------------------------------------------------------------------------
// Output: per-request result
// ---------------------------------------------------------------------------

type resultRecord struct {
	ReqID                  string   `json:"req_id"`
	Model                  string   `json:"model"`
	Latency                float64  `json:"latency"`
	Success                bool     `json:"success"`
	Error                  string   `json:"error"`
	EndTime                float64  `json:"end_time"`
	InputLen               int      `json:"input_len"`
	OutputLen              int      `json:"output_len"`
	ActualPromptTokens     *int     `json:"actual_prompt_tokens"`
	ActualCompletionTokens *int     `json:"actual_completion_tokens"`
	TensorParallelSz       int      `json:"tensor_parallel_size"`
}

type dispatchDoneMeta struct {
	Type           string  `json:"__type__"`
	LastFireTime   float64 `json:"last_fire_time"`
	AdjustedRunT0  float64 `json:"adjusted_run_t0"` // effective run_t0 after startup compensation
}

// ---------------------------------------------------------------------------
// Payload builders — mirrors replay_client.py send_request() exactly
// ---------------------------------------------------------------------------

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatPayloadDeterministic struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	MaxTokens   int           `json:"max_tokens"`
	MinTokens   int           `json:"min_tokens"`
	Temperature float64       `json:"temperature"`
	IgnoreEOS   bool          `json:"ignore_eos"`
	TP          *int          `json:"tensor_parallel_size,omitempty"`
}

type chatPayloadNatural struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	MaxTokens   int           `json:"max_tokens"`
	Temperature float64       `json:"temperature"`
	TP          *int          `json:"tensor_parallel_size,omitempty"`
}

type completionPayloadDeterministic struct {
	Model       string  `json:"model"`
	Prompt      string  `json:"prompt"`
	MaxTokens   int     `json:"max_tokens"`
	MinTokens   int     `json:"min_tokens"`
	Temperature float64 `json:"temperature"`
	IgnoreEOS   bool    `json:"ignore_eos"`
	TP          *int    `json:"tensor_parallel_size,omitempty"`
}

type completionPayloadNatural struct {
	Model       string  `json:"model"`
	Prompt      string  `json:"prompt"`
	MaxTokens   int     `json:"max_tokens"`
	Temperature float64 `json:"temperature"`
	TP          *int    `json:"tensor_parallel_size,omitempty"`
}

func buildPayload(req traceRequest, generationMode string, includeTP bool) ([]byte, string, error) {
	var tp *int
	if includeTP {
		v := req.TensorParallelSz
		tp = &v
	}

	mode := req.Mode
	if mode == "" {
		mode = "chat"
	}

	var endpoint string
	var payload interface{}

	if mode == "chat" {
		endpoint = "/v1/chat/completions"
		msgs := []chatMessage{{Role: "user", Content: req.Prompt}}
		if generationMode == "deterministic" {
			payload = chatPayloadDeterministic{
				Model:       req.Model,
				Messages:    msgs,
				MaxTokens:   req.OutputLen,
				MinTokens:   req.OutputLen,
				Temperature: 0.7,
				IgnoreEOS:   true,
				TP:          tp,
			}
		} else {
			payload = chatPayloadNatural{
				Model:       req.Model,
				Messages:    msgs,
				MaxTokens:   req.OutputLen,
				Temperature: 0.7,
				TP:          tp,
			}
		}
	} else {
		endpoint = "/v1/completions"
		if generationMode == "deterministic" {
			payload = completionPayloadDeterministic{
				Model:       req.Model,
				Prompt:      req.Prompt,
				MaxTokens:   req.OutputLen,
				MinTokens:   req.OutputLen,
				Temperature: 0.7,
				IgnoreEOS:   true,
				TP:          tp,
			}
		} else {
			payload = completionPayloadNatural{
				Model:       req.Model,
				Prompt:      req.Prompt,
				MaxTokens:   req.OutputLen,
				Temperature: 0.7,
				TP:          tp,
			}
		}
	}

	body, err := json.Marshal(payload)
	return body, endpoint, err
}

// ---------------------------------------------------------------------------
// Response parsing — extract usage.prompt_tokens / usage.completion_tokens
// ---------------------------------------------------------------------------

type usageBlock struct {
	PromptTokens     *int `json:"prompt_tokens"`
	CompletionTokens *int `json:"completion_tokens"`
}

type responseBody struct {
	Usage *usageBlock `json:"usage"`
}

// ---------------------------------------------------------------------------
// Main dispatcher
// ---------------------------------------------------------------------------

// newHTTPClient creates an independent HTTP client with its own connection pool.
// Using separate transports per dispatch worker eliminates contention on the
// transport's internal mutex (idle conn list, dial queue) which becomes the
// bottleneck above ~30K RPS with a single shared transport.
func newHTTPClient(concurrencyPerClient int, timeoutSec float64) *http.Client {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          concurrencyPerClient,
		MaxIdleConnsPerHost:   concurrencyPerClient,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     false,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   time.Duration(timeoutSec * float64(time.Second)),
	}
}

func run() int {
	startupTime := time.Now()

	baseURLsFlag := flag.String("base-urls", "", "Comma-separated target base URLs (required)")
	runT0Flag := flag.Float64("run-t0", 0, "Run start time as Unix epoch float (required)")
	generationMode := flag.String("generation-mode", "deterministic", "deterministic or natural")
	includeTP := flag.Bool("include-tp", false, "Include tensor_parallel_size in payloads")
	timeoutSec := flag.Float64("timeout", 3600.0, "Per-request timeout in seconds")
	concurrency := flag.Int("concurrency", 2000, "Max in-flight requests")
	dispatchWorkers := flag.Int("dispatch-workers", 4, "Number of parallel dispatch goroutines. "+
		"Each goroutine handles every Nth request so its per-request interval is N× longer, "+
		"eliminating the serial dispatch bottleneck at high RPS.")
	traceFile := flag.String("trace-file", "", "Input trace partition JSONL (required)")
	resultFile := flag.String("result-file", "", "Output results JSONL (required)")
	flag.Parse()

	if *baseURLsFlag == "" || *traceFile == "" || *resultFile == "" || *runT0Flag == 0 {
		fmt.Fprintln(os.Stderr, "ERROR: --base-urls, --run-t0, --trace-file, --result-file are required")
		flag.Usage()
		return 1
	}

	// Ensure Go uses all available cores and reduce GC frequency.
	// On HPC nodes GOMAXPROCS may default low; force it to NumCPU.
	runtime.GOMAXPROCS(runtime.NumCPU())
	// GOGC=200 reduces GC frequency (fewer STW pauses) at the cost of ~2x
	// memory.  At high RPS, GC pauses cause dispatch jitter.
	if os.Getenv("GOGC") == "" {
		// debug.SetGCPercent is in runtime/debug, but we can set via env
		// before any allocation pressure.  Use a simple approach:
		os.Setenv("GOGC", "200")
	}
	fmt.Fprintf(os.Stderr, "[go_dispatch] GOMAXPROCS=%d\n", runtime.GOMAXPROCS(0))

	baseURLs := strings.Split(*baseURLsFlag, ",")
	for i := range baseURLs {
		baseURLs[i] = strings.TrimRight(strings.TrimSpace(baseURLs[i]), "/")
	}

	// ------------------------------------------------------------------
	// Load trace partition
	// ------------------------------------------------------------------
	f, err := os.Open(*traceFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot open trace file %s: %v\n", *traceFile, err)
		return 1
	}
	var requests []preparedRequest
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 16*1024*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		// Skip metadata lines
		if bytes.Contains(line, []byte(`"__type__"`)) {
			continue
		}
		var req traceRequest
		if err := json.Unmarshal(line, &req); err != nil {
			fmt.Fprintf(os.Stderr, "WARN: skipping malformed trace line: %v\n", err)
			continue
		}
		body, endpoint, berr := buildPayload(req, *generationMode, *includeTP)
		if berr != nil {
			fmt.Fprintf(os.Stderr, "WARN: skipping request %s: payload build error: %v\n", req.ReqID, berr)
			continue
		}
		requests = append(requests, preparedRequest{traceRequest: req, body: body, endpoint: endpoint})
	}
	f.Close()
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: reading trace file: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "[go_dispatch] Loaded %d requests from %s\n", len(requests), *traceFile)

	// Compensate for startup latency (flag parsing + trace loading).
	startupElapsed := time.Since(startupTime)
	runT0Adjusted := startupTime.Add(startupElapsed).Add(50 * time.Millisecond)
	runT0 := runT0Adjusted.UnixNano()
	fmt.Fprintf(os.Stderr, "[go_dispatch] Startup took %.3fs; run_t0 shifted forward by %.3fs\n",
		startupElapsed.Seconds(), startupElapsed.Seconds()+0.05)

	// ------------------------------------------------------------------
	// Concurrency semaphore
	// ------------------------------------------------------------------
	sem := make(chan struct{}, *concurrency)

	// ------------------------------------------------------------------
	// Signal handling
	// ------------------------------------------------------------------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "[go_dispatch] Interrupt received, draining in-flight requests...")
		cancel()
	}()

	// ------------------------------------------------------------------
	// URL round-robin via atomic counter
	// ------------------------------------------------------------------
	var urlIdx atomic.Uint64

	// ------------------------------------------------------------------
	// Track last dispatch time for metadata line
	// ------------------------------------------------------------------
	var lastFireTime atomic.Value
	lastFireTime.Store(float64(0))

	// ------------------------------------------------------------------
	// Parallel dispatch goroutines
	//
	// Key optimizations vs. the single-transport design:
	//
	// 1. Per-worker http.Client/Transport — eliminates mutex contention
	//    on the shared connection pool (the #1 bottleneck at >30K RPS).
	//
	// 2. runtime.LockOSThread() — pins each dispatch goroutine to a
	//    dedicated OS thread so the hot-spin loop isn't preempted by the
	//    Go scheduler.  This gives nanosecond-accurate timing.
	//
	// 3. Per-worker result slices — no shared mutex during dispatch.
	//    Results are merged after all workers complete.
	//
	// 4. Reusable timers — time.NewTimer+Reset instead of time.After
	//    (which allocates a new timer per call → GC pressure).
	// ------------------------------------------------------------------
	N := *dispatchWorkers
	if N < 1 {
		N = 1
	}
	fmt.Fprintf(os.Stderr, "[go_dispatch] dispatch_workers=%d  concurrency=%d  requests=%d\n",
		N, *concurrency, len(requests))

	// Build N interleaved partitions
	partitions := make([][]preparedRequest, N)
	for i, req := range requests {
		w := i % N
		partitions[w] = append(partitions[w], req)
	}

	// Per-worker result slices with per-worker mutexes.
	// This replaces the single shared resultsMu — contention is reduced
	// by N× since each worker's goroutines only compete with siblings.
	workerResults := make([][]resultRecord, N)
	workerMus := make([]sync.Mutex, N)
	for i := range workerResults {
		workerResults[i] = make([]resultRecord, 0, len(partitions[i]))
	}

	// Per-worker WaitGroups to track in-flight requests per worker
	workerWgs := make([]sync.WaitGroup, N)

	// Per-worker HTTP clients with independent connection pools
	connsPerWorker := *concurrency / N
	if connsPerWorker < 100 {
		connsPerWorker = 100
	}
	clients := make([]*http.Client, N)
	for i := 0; i < N; i++ {
		clients[i] = newHTTPClient(connsPerWorker, *timeoutSec)
	}

	var dispatchWg sync.WaitGroup
	for w := 0; w < N; w++ {
		dispatchWg.Add(1)
		go func(workerID int, partition []preparedRequest) {
			defer dispatchWg.Done()

			// Pin this dispatch goroutine to a dedicated OS thread.
			// This prevents the Go scheduler from preempting us during
			// the hot-spin loop, which would cause multi-µs jitter.
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()

			workerClient := clients[workerID]

			// Reusable timer to avoid per-request allocation from time.After
			sleepTimer := time.NewTimer(0)
			if !sleepTimer.Stop() {
				<-sleepTimer.C
			}

			for _, req := range partition {
				if ctx.Err() != nil {
					break
				}

				targetTime := time.Unix(0, runT0+int64(req.Timestamp*1e9))

				// Coarse sleep for waits > 1 ms
				if remaining := targetTime.Sub(time.Now()); remaining > time.Millisecond {
					coarse := remaining - 500*time.Microsecond
					sleepTimer.Reset(coarse)
					select {
					case <-sleepTimer.C:
					case <-ctx.Done():
						if !sleepTimer.Stop() {
							<-sleepTimer.C
						}
						break
					}
				}

				// Hot spin for the last ≤ 1 ms — no Gosched to avoid jitter
				for time.Now().Before(targetTime) {
					if ctx.Err() != nil {
						break
					}
				}

				if ctx.Err() != nil {
					break
				}

				// Record fire time
				fireTime := time.Now().UnixNano()
				fireTimeF := float64(fireTime) / 1e9
				for {
					prev := lastFireTime.Load().(float64)
					if fireTimeF <= prev || lastFireTime.CompareAndSwap(prev, fireTimeF) {
						break
					}
				}

				// Acquire semaphore slot
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					break
				}
				if ctx.Err() != nil {
					break
				}

				// Choose URL
				idx := urlIdx.Add(1) - 1
				baseURL := baseURLs[idx%uint64(len(baseURLs))]

				workerWgs[workerID].Add(1)
				reqCopy := req
				wID := workerID
				go func(r preparedRequest, base string, ft float64) {
					defer workerWgs[wID].Done()
					defer func() { <-sem }()
					rec := doRequest(ctx, workerClient, base, r, ft)
					workerMus[wID].Lock()
					workerResults[wID] = append(workerResults[wID], rec)
					workerMus[wID].Unlock()
				}(reqCopy, baseURL, fireTimeF)
			}
		}(w, partitions[w])
	}

	// Wait for all dispatch goroutines to finish scheduling
	dispatchWg.Wait()

	// Wait for all in-flight requests to complete
	for i := 0; i < N; i++ {
		workerWgs[i].Wait()
	}

	// ------------------------------------------------------------------
	// Merge per-worker results and write JSONL
	// ------------------------------------------------------------------
	saveStart := time.Now()
	totalResults := 0
	for _, wr := range workerResults {
		totalResults += len(wr)
	}
	fmt.Fprintf(os.Stderr, "[go_dispatch] Saving %d results to %s ...\n", totalResults, *resultFile)

	out, err := os.Create(*resultFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot create result file %s: %v\n", *resultFile, err)
		return 1
	}
	enc := json.NewEncoder(out)
	for _, wr := range workerResults {
		for _, rec := range wr {
			if encErr := enc.Encode(rec); encErr != nil {
				fmt.Fprintf(os.Stderr, "WARN: failed to encode result record: %v\n", encErr)
			}
		}
	}

	// Write dispatch_done metadata line
	meta := dispatchDoneMeta{
		Type:          "dispatch_done",
		LastFireTime:  lastFireTime.Load().(float64),
		AdjustedRunT0: float64(runT0) / 1e9,
	}
	if encErr := enc.Encode(meta); encErr != nil {
		fmt.Fprintf(os.Stderr, "WARN: failed to encode dispatch_done metadata: %v\n", encErr)
	}
	out.Close()

	saveElapsed := time.Since(saveStart)
	fmt.Fprintf(os.Stderr, "[go_dispatch] Done. %d results written to %s (save took %.2fs)\n", totalResults, *resultFile, saveElapsed.Seconds())
	return 0
}

func doRequest(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	req preparedRequest,
	fireTime float64,
) resultRecord {
	rec := resultRecord{
		ReqID:        req.ReqID,
		Model:        req.Model,
		InputLen:     req.InputLen,
		OutputLen:    req.OutputLen,
		TensorParallelSz: req.TensorParallelSz,
	}

	url := baseURL + req.endpoint
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(req.body))
	if err != nil {
		rec.Error = fmt.Sprintf("request build error: %v", err)
		rec.EndTime = float64(time.Now().UnixNano()) / 1e9
		rec.Latency = rec.EndTime - fireTime
		return rec
	}
	httpReq.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := client.Do(httpReq)
	endTime := float64(time.Now().UnixNano()) / 1e9
	rec.EndTime = endTime
	rec.Latency = endTime - fireTime

	if err != nil {
		if ctx.Err() != nil {
			rec.Error = "context_cancelled"
		} else {
			rec.Error = fmt.Sprintf("%T: %v", err, err)
		}
		return rec
	}

	_ = start

	respBody, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snippet := string(respBody)
		if len(snippet) > 300 {
			snippet = snippet[:300]
		}
		rec.Error = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, snippet)
		return rec
	}

	if readErr != nil {
		rec.Error = fmt.Sprintf("body read error: %v", readErr)
		return rec
	}

	rec.Success = true

	// Parse usage tokens
	var rb responseBody
	if jsonErr := json.Unmarshal(respBody, &rb); jsonErr == nil && rb.Usage != nil {
		rec.ActualPromptTokens = rb.Usage.PromptTokens
		rec.ActualCompletionTokens = rb.Usage.CompletionTokens
	}

	return rec
}

func main() {
	os.Exit(run())
}

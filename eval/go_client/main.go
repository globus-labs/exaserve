// go_dispatch: high-throughput HTTP request dispatcher for replay_client.py.
//
// The client now distinguishes between:
//   - active requests        (--max-active-requests)
//   - queued requests        (--queue-capacity)
//   - transport connections  (--max-conns-per-host)
//
// This makes client-side outstanding work explicit and allows the caller to
// reason about queueing, transport reuse, and throughput separately.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

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

type preparedRequest struct {
	traceRequest
	body     []byte
	endpoint string
}

type resultRecord struct {
	ReqID                  string  `json:"req_id"`
	Model                  string  `json:"model"`
	Latency                float64 `json:"latency"`
	Success                bool    `json:"success"`
	Error                  string  `json:"error"`
	ErrorClass             string  `json:"error_class,omitempty"`
	StatusCode             int     `json:"status_code,omitempty"`
	EndTime                float64 `json:"end_time"`
	InputLen               int     `json:"input_len"`
	OutputLen              int     `json:"output_len"`
	ActualPromptTokens     *int    `json:"actual_prompt_tokens"`
	ActualCompletionTokens *int    `json:"actual_completion_tokens"`
	TensorParallelSz       int     `json:"tensor_parallel_size"`
	ScheduledAt            float64 `json:"scheduled_at,omitempty"`
	EnqueuedAt             float64 `json:"enqueued_at,omitempty"`
	DequeuedAt             float64 `json:"dequeued_at,omitempty"`
	RequestStartAt         float64 `json:"request_start_at,omitempty"`
	HeadersAt              float64 `json:"headers_at,omitempty"`
	BodyDoneAt             float64 `json:"body_done_at,omitempty"`
}

type dispatchDoneMeta struct {
	Type               string  `json:"__type__"`
	LastFireTime       float64 `json:"last_fire_time"`
	LastRequestStartAt float64 `json:"last_request_start_at"`
	LastBodyDoneAt     float64 `json:"last_body_done_at"`
	AdjustedRunT0      float64 `json:"adjusted_run_t0"`
}

type summaryRecord struct {
	Type               string  `json:"__type__"`
	RequestsCompleted  int     `json:"requests_completed"`
	RequestsScheduled  int     `json:"requests_scheduled"`
	Errors             int     `json:"errors"`
	P50S               float64 `json:"p50_s"`
	P99S               float64 `json:"p99_s"`
	TotalInputTokens   int64   `json:"total_input_tokens"`
	TotalOutputTokens  int64   `json:"total_output_tokens"`
	LastFireTime       float64 `json:"last_fire_time"`
	LastRequestStartAt float64 `json:"last_request_start_at"`
	LastBodyDoneAt     float64 `json:"last_body_done_at"`
	AdjustedRunT0      float64 `json:"adjusted_run_t0"`
}

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

type usageBlock struct {
	PromptTokens     *int `json:"prompt_tokens"`
	CompletionTokens *int `json:"completion_tokens"`
}

type responseBody struct {
	Usage *usageBlock `json:"usage"`
}

type workItem struct {
	req             preparedRequest
	target          string
	scheduledAt     float64
	enqueuedAt      float64
	workerID        int
	resultIndex     int
	samplePhases    bool
	enableConnTrace bool
}

func buildPayload(req traceRequest, generationMode string, includeTP bool) ([]byte, string, error) {
	var tp *int
	if includeTP {
		value := req.TensorParallelSz
		tp = &value
	}

	mode := req.Mode
	if mode == "" {
		mode = "chat"
	}

	var endpoint string
	var payload any
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

func newHTTPClient(idleConnsPerClient int, maxConnsPerHost int, timeoutSec float64) *http.Client {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          idleConnsPerClient,
		MaxIdleConnsPerHost:   idleConnsPerClient,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     false,
	}
	if maxConnsPerHost > 0 {
		transport.MaxConnsPerHost = maxConnsPerHost
	}
	return &http.Client{
		Transport: transport,
		Timeout:   time.Duration(timeoutSec * float64(time.Second)),
	}
}

func run() int {
	startupTime := time.Now()

	baseURLsFlag := flag.String("base-urls", "", "Comma-separated target base URLs (required)")
	generationMode := flag.String("generation-mode", "deterministic", "deterministic or natural")
	includeTP := flag.Bool("include-tp", false, "Include tensor_parallel_size in payloads")
	timeoutSec := flag.Float64("timeout", 3600.0, "Per-request timeout in seconds")
	maxActiveRequests := flag.Int("max-active-requests", 0, "Maximum active in-flight HTTP requests")
	legacyConcurrency := flag.Int("concurrency", 0, "Deprecated alias for --max-active-requests")
	queueCapacity := flag.Int("queue-capacity", 0, "Buffered queue capacity beyond active requests")
	maxConnsPerHost := flag.Int("max-conns-per-host", 0, "Maximum transport connections per host (0 = derive from active requests)")
	metricsFile := flag.String("metrics-file", "", "Optional JSON metrics output path")
	phaseTraceFile := flag.String("phase-trace-file", "", "Optional JSONL sampled phase trace output path")
	phaseTraceSampleRate := flag.Float64("phase-trace-sample-rate", 0.0, "Probability [0,1] for writing a per-request phase trace")
	enableHTTPTrace := flag.Bool("enable-httptrace", false, "Enable aggregate httptrace connection telemetry")
	numGoWorkers := flag.Int("num-go-workers", 4, "Number of dispatch schedulers")
	traceFile := flag.String("trace-file", "", "Input trace partition JSONL (required)")
	resultFile := flag.String("result-file", "", "Output results JSONL (required)")
	sumOnly := flag.Bool("sum-only", false, "Write only a summary line instead of per-request results")
	workerID := flag.String("worker-id", "", "Worker identifier for log prefixes")
	warmupRPS := flag.Int("warmup-rps", 0, "Warm-up requests per second (0 = no warmup)")
	warmupDuration := flag.Float64("warmup-duration", 0, "Warm-up duration in seconds")
	cpuprofileFlag := flag.String("cpuprofile", "", "Write CPU profile to this file")
	flag.Parse()

	if *cpuprofileFlag != "" {
		profileFile, err := os.Create(*cpuprofileFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: could not create CPU profile: %v\n", err)
			return 1
		}
		if err := pprof.StartCPUProfile(profileFile); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: could not start CPU profile: %v\n", err)
			profileFile.Close()
			return 1
		}
		defer func() {
			pprof.StopCPUProfile()
			profileFile.Close()
		}()
	}

	if *baseURLsFlag == "" || *traceFile == "" || *resultFile == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --base-urls, --trace-file, --result-file are required")
		flag.Usage()
		return 1
	}
	if *phaseTraceSampleRate < 0 || *phaseTraceSampleRate > 1 {
		fmt.Fprintln(os.Stderr, "ERROR: --phase-trace-sample-rate must be in [0, 1]")
		return 1
	}

	resolvedMaxActive, err := resolveMaxActiveRequests(*maxActiveRequests, *legacyConcurrency)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}
	if resolvedMaxActive < 1 {
		fmt.Fprintln(os.Stderr, "ERROR: max active requests must be >= 1")
		return 1
	}
	if *queueCapacity < 0 {
		fmt.Fprintln(os.Stderr, "ERROR: --queue-capacity must be >= 0")
		return 1
	}

	logPrefix := "[go_dispatch]"
	if *workerID != "" {
		logPrefix = fmt.Sprintf("[go_dispatch %s]", *workerID)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())
	if os.Getenv("GOGC") == "" {
		os.Setenv("GOGC", "200")
	}
	fmt.Fprintf(os.Stderr, "%s GOMAXPROCS=%d\n", logPrefix, runtime.GOMAXPROCS(0))

	baseURLs := normalizeBaseURLs(*baseURLsFlag)
	if len(baseURLs) == 0 {
		fmt.Fprintln(os.Stderr, "ERROR: --base-urls did not contain any valid URL")
		return 1
	}
	resolvedMaxConns := *maxConnsPerHost
	if resolvedMaxConns <= 0 {
		resolvedMaxConns = resolvedMaxActive
	}

	traceRequests, err := loadRequests(*traceFile, *generationMode, *includeTP)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "%s Loaded %d requests from %s\n", logPrefix, len(traceRequests), *traceFile)
	fmt.Fprintf(
		os.Stderr,
		"%s Startup took %.3fs | max_active=%d queue_capacity=%d max_conns=%d\n",
		logPrefix,
		time.Since(startupTime).Seconds(),
		resolvedMaxActive,
		*queueCapacity,
		resolvedMaxConns,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintf(os.Stderr, "%s Interrupt received, draining in-flight requests...\n", logPrefix)
		cancel()
	}()

	if *warmupRPS > 0 && *warmupDuration > 0 && len(traceRequests) > 0 {
		warmupCount := int(float64(*warmupRPS) * *warmupDuration)
		fmt.Fprintf(
			os.Stderr,
			"%s Starting warm-up: %d RPS x %.1fs = %d requests\n",
			logPrefix,
			*warmupRPS,
			*warmupDuration,
			warmupCount,
		)
		warmupClient := newHTTPClient(max(1, *warmupRPS), max(1, *warmupRPS), *timeoutSec)
		template := traceRequests[0]
		var warmupWg sync.WaitGroup
		interval := time.Second / time.Duration(*warmupRPS)
		for idx := 0; idx < warmupCount; idx++ {
			if ctx.Err() != nil {
				break
			}
			target := baseURLs[idx%len(baseURLs)]
			warmupWg.Add(1)
			go func(target string) {
				defer warmupWg.Done()
				item := workItem{
					req:         template,
					target:      target,
					scheduledAt: nowSeconds(),
					enqueuedAt:  nowSeconds(),
				}
				_ = doRequest(ctx, warmupClient, item, nil)
			}(target)
			if idx < warmupCount-1 {
				time.Sleep(interval)
			}
		}
		warmupWg.Wait()
		fmt.Fprintf(os.Stderr, "%s Warm-up done in %.2fs\n", logPrefix, *warmupDuration)
	}

	fmt.Println("GO_CLI_READY")

	runT0, err := readRunT0()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}
	runT0Time := time.Unix(0, int64(runT0*1e9))
	fmt.Fprintf(os.Stderr, "%s Received run_t0=%.6f from stdin\n", logPrefix, runT0)

	numSchedulers := *numGoWorkers
	if numSchedulers < 1 {
		numSchedulers = 1
	}
	partitions := make([][]preparedRequest, numSchedulers)
	for idx, req := range traceRequests {
		partitions[idx%numSchedulers] = append(partitions[idx%numSchedulers], req)
	}
	workerResults := make([][]resultRecord, numSchedulers)
	for idx := range workerResults {
		workerResults[idx] = make([]resultRecord, len(partitions[idx]))
	}

	perClientActive := ceilDiv(resolvedMaxActive, numSchedulers)
	perClientConns := ceilDiv(resolvedMaxConns, numSchedulers)
	if perClientConns < 1 {
		perClientConns = 1
	}
	clients := make([]*http.Client, numSchedulers)
	for idx := 0; idx < numSchedulers; idx++ {
		clients[idx] = newHTTPClient(perClientActive, perClientConns, *timeoutSec)
	}

	collector := newMetricsCollector(*workerID, resolvedMaxActive, *queueCapacity, resolvedMaxConns, *enableHTTPTrace)
	workCh := make(chan workItem, *queueCapacity)
	outstandingSlots := make(chan struct{}, resolvedMaxActive+*queueCapacity)
	var poolWg sync.WaitGroup
	for idx := 0; idx < resolvedMaxActive; idx++ {
		clientIdx := idx % numSchedulers
		poolWg.Add(1)
		go func(client *http.Client) {
			defer poolWg.Done()
			for item := range workCh {
				rec := doRequest(ctx, client, item, collector)
				workerResults[item.workerID][item.resultIndex] = rec
				<-outstandingSlots
				collector.DecOutstanding()
			}
		}(clients[clientIdx])
	}

	fmt.Fprintf(
		os.Stderr,
		"%s Launching dispatch group at T0+%.3fs (active=%d queue=%d)\n",
		logPrefix,
		time.Since(runT0Time).Seconds(),
		resolvedMaxActive,
		*queueCapacity,
	)

	var dispatchWg sync.WaitGroup
	for workerIdx := 0; workerIdx < numSchedulers; workerIdx++ {
		dispatchWg.Add(1)
		go func(dispatchWorkerID int, partition []preparedRequest) {
			defer dispatchWg.Done()
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()

			sleepTimer := time.NewTimer(0)
			if !sleepTimer.Stop() {
				<-sleepTimer.C
			}

			var localURLIdx uint64
			for resultIdx, req := range partition {
				if ctx.Err() != nil {
					break
				}

				targetTime := time.Unix(0, int64(runT0*1e9)+int64(req.Timestamp*1e9))
				if remaining := targetTime.Sub(time.Now()); remaining > time.Millisecond {
					coarse := remaining - 500*time.Microsecond
					sleepTimer.Reset(coarse)
					select {
					case <-sleepTimer.C:
					case <-ctx.Done():
						if !sleepTimer.Stop() {
							<-sleepTimer.C
						}
						return
					}
				}
				for time.Now().Before(targetTime) {
					if ctx.Err() != nil {
						return
					}
				}
				if ctx.Err() != nil {
					return
				}

				readyAt := time.Now()
				collector.ObserveDispatchLag(readyAt.Sub(targetTime))
				select {
				case outstandingSlots <- struct{}{}:
					collector.IncOutstanding()
				case <-ctx.Done():
					return
				}

				target := baseURLs[localURLIdx%uint64(len(baseURLs))]
				localURLIdx++
				samplePhases := *phaseTraceFile != "" && *phaseTraceSampleRate > 0 && rand.Float64() <= *phaseTraceSampleRate
				item := workItem{
					req:             req,
					target:          target,
					scheduledAt:     float64(targetTime.UnixNano()) / 1e9,
					enqueuedAt:      nowSeconds(),
					workerID:        dispatchWorkerID,
					resultIndex:     resultIdx,
					samplePhases:    samplePhases,
					enableConnTrace: *enableHTTPTrace,
				}

				workCh <- item
				collector.ObserveQueueDepth(len(workCh))
			}
		}(workerIdx, partitions[workerIdx])
	}

	dispatchWg.Wait()
	dispatchElapsed := time.Since(runT0Time)
	fmt.Fprintf(os.Stderr, "%s All requests dispatched in %.3fs\n", logPrefix, dispatchElapsed.Seconds())

	close(workCh)
	poolWg.Wait()
	totalElapsed := time.Since(runT0Time)
	fmt.Fprintf(
		os.Stderr,
		"%s All requests completed in %.3fs (drain %.3fs)\n",
		logPrefix,
		totalElapsed.Seconds(),
		totalElapsed.Seconds()-dispatchElapsed.Seconds(),
	)

	lastRequestStartAt := float64(collector.lastRequestStartNs.Load()) / 1e9
	lastBodyDoneAt := float64(collector.lastBodyDoneNs.Load()) / 1e9
	lastFireTime := lastRequestStartAt
	if lastFireTime <= 0 {
		lastFireTime = runT0
	}

	if err := writeResults(*resultFile, *sumOnly, workerResults, lastFireTime, lastRequestStartAt, lastBodyDoneAt, runT0, logPrefix); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}
	if err := collector.WriteMetrics(*metricsFile, runT0, len(traceRequests), len(traceRequests)); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: could not write metrics: %v\n", err)
		return 1
	}
	if err := collector.WritePhaseTraces(*phaseTraceFile); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: could not write phase traces: %v\n", err)
		return 1
	}
	return 0
}

func doRequest(ctx context.Context, client *http.Client, item workItem, collector *metricsCollector) resultRecord {
	rec := resultRecord{
		ReqID:            item.req.ReqID,
		Model:            item.req.Model,
		InputLen:         item.req.InputLen,
		OutputLen:        item.req.OutputLen,
		TensorParallelSz: item.req.TensorParallelSz,
		ScheduledAt:      item.scheduledAt,
		EnqueuedAt:       item.enqueuedAt,
	}

	var sampledTrace *phaseTraceRecord
	var traceState httpTraceState
	useHTTPTrace := item.enableConnTrace || item.samplePhases
	if item.samplePhases {
		sampledTrace = &phaseTraceRecord{
			ReqID:            item.req.ReqID,
			Target:           item.target,
			Endpoint:         item.req.endpoint,
			ScheduledAt:      item.scheduledAt,
			EnqueuedAt:       item.enqueuedAt,
			HTTPTraceEnabled: useHTTPTrace,
		}
	}

	if collector != nil {
		collector.IncActive()
		defer collector.DecActive()
	}

	dequeuedAt := nowSeconds()
	rec.DequeuedAt = dequeuedAt
	queueWait := durationBetween(rec.EnqueuedAt, rec.DequeuedAt)
	if collector != nil {
		collector.ObserveQueueWait(queueWait)
	}
	if sampledTrace != nil {
		sampledTrace.DequeuedAt = rec.DequeuedAt
		sampledTrace.QueueWaitS = queueWait.Seconds()
	}

	url := item.target + item.req.endpoint
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(item.req.body))
	if err != nil {
		rec.Error = fmt.Sprintf("request build error: %v", err)
		rec.ErrorClass = "build"
		rec.BodyDoneAt = nowSeconds()
		rec.EndTime = rec.BodyDoneAt
		rec.Latency = rec.EndTime - rec.EnqueuedAt
		if sampledTrace != nil {
			sampledTrace.BodyDoneAt = rec.BodyDoneAt
			sampledTrace.ErrorClass = rec.ErrorClass
			sampledTrace.SlotHoldS = durationBetween(rec.EnqueuedAt, rec.BodyDoneAt).Seconds()
		}
		if collector != nil {
			collector.RecordCompletion(item.target, rec, sampledTrace)
		}
		return rec
	}
	httpReq.Header.Set("Content-Type", "application/json")

	if useHTTPTrace {
		httpReq = httpReq.WithContext(httptrace.WithClientTrace(httpReq.Context(), buildClientTrace(useHTTPTrace, item.target, collector, &traceState)))
	}

	requestStart := time.Now()
	rec.RequestStartAt = float64(requestStart.UnixNano()) / 1e9
	if sampledTrace != nil {
		sampledTrace.RequestStartAt = rec.RequestStartAt
		sampledTrace.DispatchLagS = durationBetween(rec.ScheduledAt, rec.RequestStartAt).Seconds()
	}

	resp, err := client.Do(httpReq)
	headersAt := time.Now()
	if !traceState.headersAt.IsZero() {
		headersAt = traceState.headersAt
	}
	rec.HeadersAt = float64(headersAt.UnixNano()) / 1e9
	if collector != nil {
		collector.ObserveTimeToHeaders(headersAt.Sub(requestStart))
	}

	if sampledTrace != nil {
		sampledTrace.HeadersAt = rec.HeadersAt
		sampledTrace.TimeToHeadersS = durationBetween(rec.RequestStartAt, rec.HeadersAt).Seconds()
		if !traceState.connectStartAt.IsZero() {
			sampledTrace.ConnectStartAt = float64(traceState.connectStartAt.UnixNano()) / 1e9
		}
		if !traceState.connectDoneAt.IsZero() {
			sampledTrace.ConnectDoneAt = float64(traceState.connectDoneAt.UnixNano()) / 1e9
		}
		sampledTrace.ReusedConnection = traceState.reused
		sampledTrace.ReusedIdle = traceState.wasIdle
		sampledTrace.NewConnection = traceState.newConnection
	}

	if err != nil {
		rec.Error = fmt.Sprintf("%T: %v", err, err)
		rec.ErrorClass = classifyRequestError(err, ctx.Err())
		rec.BodyDoneAt = nowSeconds()
		rec.EndTime = rec.BodyDoneAt
		rec.Latency = rec.EndTime - rec.EnqueuedAt
		if sampledTrace != nil {
			sampledTrace.BodyDoneAt = rec.BodyDoneAt
			sampledTrace.BodyReadS = 0
			sampledTrace.SlotHoldS = durationBetween(rec.EnqueuedAt, rec.BodyDoneAt).Seconds()
			sampledTrace.ErrorClass = rec.ErrorClass
		}
		if collector != nil {
			collector.ObserveSlotHold(durationBetween(rec.EnqueuedAt, rec.BodyDoneAt))
			collector.RecordCompletion(item.target, rec, sampledTrace)
		}
		return rec
	}
	defer resp.Body.Close()

	rec.StatusCode = resp.StatusCode
	respBody, readErr := io.ReadAll(resp.Body)
	bodyDone := time.Now()
	rec.BodyDoneAt = float64(bodyDone.UnixNano()) / 1e9
	rec.EndTime = rec.BodyDoneAt
	rec.Latency = rec.EndTime - rec.EnqueuedAt

	if collector != nil {
		collector.ObserveBodyRead(bodyDone.Sub(headersAt))
		collector.ObserveSlotHold(durationBetween(rec.EnqueuedAt, rec.BodyDoneAt))
	}
	if sampledTrace != nil {
		sampledTrace.BodyDoneAt = rec.BodyDoneAt
		sampledTrace.BodyReadS = durationBetween(rec.HeadersAt, rec.BodyDoneAt).Seconds()
		sampledTrace.SlotHoldS = durationBetween(rec.EnqueuedAt, rec.BodyDoneAt).Seconds()
		sampledTrace.StatusCode = rec.StatusCode
	}

	if resp.StatusCode != http.StatusOK {
		snippet := string(respBody)
		if len(snippet) > 300 {
			snippet = snippet[:300]
		}
		rec.Error = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, snippet)
		rec.ErrorClass = "http_status"
		if sampledTrace != nil {
			sampledTrace.ErrorClass = rec.ErrorClass
		}
		if collector != nil {
			collector.RecordCompletion(item.target, rec, sampledTrace)
		}
		return rec
	}
	if readErr != nil {
		rec.Error = fmt.Sprintf("body read error: %v", readErr)
		rec.ErrorClass = "body_read"
		if sampledTrace != nil {
			sampledTrace.ErrorClass = rec.ErrorClass
		}
		if collector != nil {
			collector.RecordCompletion(item.target, rec, sampledTrace)
		}
		return rec
	}

	rec.Success = true
	var rb responseBody
	if jsonErr := json.Unmarshal(respBody, &rb); jsonErr == nil && rb.Usage != nil {
		rec.ActualPromptTokens = rb.Usage.PromptTokens
		rec.ActualCompletionTokens = rb.Usage.CompletionTokens
	}
	if sampledTrace != nil {
		sampledTrace.Success = true
	}
	if collector != nil {
		collector.RecordCompletion(item.target, rec, sampledTrace)
	}
	return rec
}

func writeResults(resultFile string, sumOnly bool, workerResults [][]resultRecord, lastFireTime float64, lastRequestStartAt float64, lastBodyDoneAt float64, runT0 float64, logPrefix string) error {
	totalResults := 0
	for _, results := range workerResults {
		for _, rec := range results {
			if recordPopulated(rec) {
				totalResults++
			}
		}
	}

	output, err := os.Create(resultFile)
	if err != nil {
		return fmt.Errorf("cannot create result file %s: %w", resultFile, err)
	}
	defer output.Close()
	encoder := json.NewEncoder(output)

	if sumOnly {
		var (
			successLatencies                    []float64
			completed, errorsCount              int
			totalInputTokens, totalOutputTokens int64
		)
		for _, results := range workerResults {
			for _, rec := range results {
				if !recordPopulated(rec) {
					continue
				}
				if rec.Success {
					completed++
					successLatencies = append(successLatencies, rec.Latency)
					if rec.ActualPromptTokens != nil {
						totalInputTokens += int64(*rec.ActualPromptTokens)
					} else {
						totalInputTokens += int64(rec.InputLen)
					}
					if rec.ActualCompletionTokens != nil {
						totalOutputTokens += int64(*rec.ActualCompletionTokens)
					} else {
						totalOutputTokens += int64(rec.OutputLen)
					}
				} else {
					errorsCount++
				}
			}
		}
		sort.Float64s(successLatencies)
		summary := summaryRecord{
			Type:               "summary",
			RequestsCompleted:  completed,
			RequestsScheduled:  totalResults,
			Errors:             errorsCount,
			P50S:               percentile(successLatencies, 0.50),
			P99S:               percentile(successLatencies, 0.99),
			TotalInputTokens:   totalInputTokens,
			TotalOutputTokens:  totalOutputTokens,
			LastFireTime:       lastFireTime,
			LastRequestStartAt: lastRequestStartAt,
			LastBodyDoneAt:     lastBodyDoneAt,
			AdjustedRunT0:      runT0,
		}
		if err := encoder.Encode(summary); err != nil {
			return fmt.Errorf("failed to encode summary: %w", err)
		}
		fmt.Fprintf(
			os.Stderr,
			"%s Summary: completed=%d errors=%d p50=%.3fs p99=%.3fs\n",
			logPrefix,
			completed,
			errorsCount,
			summary.P50S,
			summary.P99S,
		)
		return nil
	}

	fmt.Fprintf(os.Stderr, "%s Saving %d results to %s ...\n", logPrefix, totalResults, resultFile)
	for _, results := range workerResults {
		for _, rec := range results {
			if !recordPopulated(rec) {
				continue
			}
			if err := encoder.Encode(rec); err != nil {
				return fmt.Errorf("failed to encode result record: %w", err)
			}
		}
	}
	meta := dispatchDoneMeta{
		Type:               "dispatch_done",
		LastFireTime:       lastFireTime,
		LastRequestStartAt: lastRequestStartAt,
		LastBodyDoneAt:     lastBodyDoneAt,
		AdjustedRunT0:      runT0,
	}
	if err := encoder.Encode(meta); err != nil {
		return fmt.Errorf("failed to encode dispatch metadata: %w", err)
	}
	return nil
}

func loadRequests(traceFile string, generationMode string, includeTP bool) ([]preparedRequest, error) {
	input, err := os.Open(traceFile)
	if err != nil {
		return nil, fmt.Errorf("cannot open trace file %s: %w", traceFile, err)
	}
	defer input.Close()

	var requests []preparedRequest
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 16*1024*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 || bytes.Contains(line, []byte(`"__type__"`)) {
			continue
		}
		var req traceRequest
		if err := json.Unmarshal(line, &req); err != nil {
			return nil, fmt.Errorf("malformed trace line: %w", err)
		}
		body, endpoint, err := buildPayload(req, generationMode, includeTP)
		if err != nil {
			return nil, fmt.Errorf("payload build error for %s: %w", req.ReqID, err)
		}
		requests = append(requests, preparedRequest{
			traceRequest: req,
			body:         body,
			endpoint:     endpoint,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading trace file: %w", err)
	}
	return requests, nil
}

func normalizeBaseURLs(raw string) []string {
	items := strings.Split(raw, ",")
	result := make([]string, 0, len(items))
	for _, item := range items {
		normalized := strings.TrimRight(strings.TrimSpace(item), "/")
		if normalized != "" {
			result = append(result, normalized)
		}
	}
	return result
}

func resolveMaxActiveRequests(maxActive int, legacy int) (int, error) {
	if maxActive > 0 && legacy > 0 && maxActive != legacy {
		return 0, errors.New("--max-active-requests and --concurrency disagree")
	}
	if maxActive > 0 {
		return maxActive, nil
	}
	if legacy > 0 {
		return legacy, nil
	}
	return 0, errors.New("--max-active-requests is required")
}

func readRunT0() (float64, error) {
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return 0, errors.New("stdin closed before receiving run_t0")
	}
	line := strings.TrimSpace(scanner.Text())
	var runT0 float64
	if _, err := fmt.Sscanf(line, "%f", &runT0); err != nil {
		return 0, fmt.Errorf("failed to parse run_t0 from stdin: %q: %w", line, err)
	}
	return runT0, nil
}

func percentile(values []float64, fraction float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if len(values) == 1 {
		return values[0]
	}
	index := int(float64(len(values)-1) * fraction)
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func writeFile(path string, data []byte) error {
	if path == "" {
		return nil
	}
	parent := filepath.Dir(path)
	if parent != "" && parent != "." {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, data, 0o644)
}

func classifyRequestError(reqErr error, ctxErr error) string {
	if ctxErr != nil {
		return "context_cancelled"
	}
	if errors.Is(reqErr, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(reqErr, &netErr) {
		if netErr.Timeout() {
			return "timeout"
		}
		return "network"
	}
	return "request"
}

func ceilDiv(value int, divisor int) int {
	if divisor <= 0 {
		return value
	}
	return (value + divisor - 1) / divisor
}

func durationBetween(start float64, end float64) time.Duration {
	if end <= 0 || start <= 0 || end < start {
		return 0
	}
	return time.Duration((end - start) * float64(time.Second))
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func recordPopulated(rec resultRecord) bool {
	return rec.ReqID != "" || rec.Success || rec.Error != "" || rec.EndTime > 0
}

func main() {
	os.Exit(run())
}

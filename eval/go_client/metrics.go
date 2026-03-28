package main

import (
	"encoding/json"
	"fmt"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"
)

var defaultHistogramBounds = []time.Duration{
	50 * time.Microsecond,
	100 * time.Microsecond,
	250 * time.Microsecond,
	500 * time.Microsecond,
	1 * time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	20 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	60 * time.Second,
}

type histogramSnapshot struct {
	BucketUpperBoundsS []float64 `json:"bucket_upper_bounds_s"`
	Counts             []uint64  `json:"counts"`
	Count              uint64    `json:"count"`
	SumS               float64   `json:"sum_s"`
}

type durationHistogram struct {
	bounds []time.Duration
	counts []atomic.Uint64
	total  atomic.Uint64
	sumNs  atomic.Int64
}

func newDurationHistogram(bounds []time.Duration) *durationHistogram {
	h := &durationHistogram{
		bounds: append([]time.Duration(nil), bounds...),
		counts: make([]atomic.Uint64, len(bounds)+1),
	}
	return h
}

func (h *durationHistogram) Observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	for idx, bound := range h.bounds {
		if d <= bound {
			h.counts[idx].Add(1)
			h.total.Add(1)
			h.sumNs.Add(int64(d))
			return
		}
	}
	h.counts[len(h.counts)-1].Add(1)
	h.total.Add(1)
	h.sumNs.Add(int64(d))
}

func (h *durationHistogram) Snapshot() histogramSnapshot {
	snapshot := histogramSnapshot{
		BucketUpperBoundsS: make([]float64, 0, len(h.bounds)+1),
		Counts:             make([]uint64, 0, len(h.counts)),
		Count:              h.total.Load(),
		SumS:               float64(h.sumNs.Load()) / float64(time.Second),
	}
	for _, bound := range h.bounds {
		snapshot.BucketUpperBoundsS = append(snapshot.BucketUpperBoundsS, bound.Seconds())
	}
	snapshot.BucketUpperBoundsS = append(snapshot.BucketUpperBoundsS, -1.0)
	for idx := range h.counts {
		snapshot.Counts = append(snapshot.Counts, h.counts[idx].Load())
	}
	return snapshot
}

type perTargetMetrics struct {
	Requests              uint64            `json:"requests"`
	Successes             uint64            `json:"successes"`
	Failures              uint64            `json:"failures"`
	NewConnections        uint64            `json:"new_connections"`
	ReusedConnections     uint64            `json:"reused_connections"`
	ReusedIdleConnections uint64            `json:"reused_idle_connections"`
	StatusCounts          map[string]uint64 `json:"status_counts"`
	ErrorCounts           map[string]uint64 `json:"error_counts"`
}

type goClientMetrics struct {
	SchemaVersion          string                       `json:"schema_version"`
	WorkerID               string                       `json:"worker_id"`
	RunT0                  float64                      `json:"run_t0"`
	StartedAt              float64                      `json:"started_at"`
	CompletedAt            float64                      `json:"completed_at"`
	RequestsLoaded         int                          `json:"requests_loaded"`
	RequestsScheduled      int                          `json:"requests_scheduled"`
	RequestsCompleted      uint64                       `json:"requests_completed"`
	RequestsSucceeded      uint64                       `json:"requests_succeeded"`
	RequestsFailed         uint64                       `json:"requests_failed"`
	EnableHTTPTrace        bool                         `json:"enable_httptrace"`
	MaxActiveRequests      int                          `json:"max_active_requests"`
	QueueCapacity          int                          `json:"queue_capacity"`
	MaxConnsPerHost        int                          `json:"max_conns_per_host"`
	MaxObservedActive      int64                        `json:"max_observed_active"`
	MaxObservedOutstanding int64                        `json:"max_observed_outstanding"`
	MaxObservedQueueDepth  int64                        `json:"max_observed_queue_depth"`
	NewConnections         uint64                       `json:"new_connections"`
	ReusedConnections      uint64                       `json:"reused_connections"`
	ReusedIdleConnections  uint64                       `json:"reused_idle_connections"`
	LastRequestStartAt     float64                      `json:"last_request_start_at"`
	LastBodyDoneAt         float64                      `json:"last_body_done_at"`
	StatusCounts           map[string]uint64            `json:"status_counts"`
	ErrorCounts            map[string]uint64            `json:"error_counts"`
	Histograms             map[string]histogramSnapshot `json:"histograms"`
	PerTarget              map[string]perTargetMetrics  `json:"per_target"`
}

type phaseTraceRecord struct {
	ReqID            string  `json:"req_id"`
	Target           string  `json:"target"`
	Endpoint         string  `json:"endpoint"`
	ScheduledAt      float64 `json:"scheduled_at"`
	EnqueuedAt       float64 `json:"enqueued_at"`
	DequeuedAt       float64 `json:"dequeued_at"`
	RequestStartAt   float64 `json:"request_start_at"`
	HeadersAt        float64 `json:"headers_at"`
	BodyDoneAt       float64 `json:"body_done_at"`
	ConnectStartAt   float64 `json:"connect_start_at,omitempty"`
	ConnectDoneAt    float64 `json:"connect_done_at,omitempty"`
	QueueWaitS       float64 `json:"queue_wait_s"`
	TimeToHeadersS   float64 `json:"time_to_headers_s"`
	BodyReadS        float64 `json:"body_read_s"`
	SlotHoldS        float64 `json:"slot_hold_s"`
	DispatchLagS     float64 `json:"dispatch_lag_s"`
	ReusedConnection bool    `json:"reused_connection"`
	ReusedIdle       bool    `json:"reused_idle"`
	NewConnection    bool    `json:"new_connection"`
	HTTPTraceEnabled bool    `json:"httptrace_enabled"`
	StatusCode       int     `json:"status_code,omitempty"`
	Success          bool    `json:"success"`
	ErrorClass       string  `json:"error_class,omitempty"`
}

type httpTraceState struct {
	headersAt      time.Time
	connectStartAt time.Time
	connectDoneAt  time.Time
	reused         bool
	wasIdle        bool
	newConnection  bool
}

type metricsCollector struct {
	workerID        string
	maxActive       int
	queueCapacity   int
	maxConnsPerHost int
	enableHTTPTrace bool
	startedAt       float64

	completedRequests atomic.Uint64
	succeededRequests atomic.Uint64
	failedRequests    atomic.Uint64

	newConnections        atomic.Uint64
	reusedConnections     atomic.Uint64
	reusedIdleConnections atomic.Uint64

	activeRequests         atomic.Int64
	maxObservedActive      atomic.Int64
	outstandingRequests    atomic.Int64
	maxObservedOutstanding atomic.Int64
	maxObservedQueueDepth  atomic.Int64

	lastRequestStartNs atomic.Int64
	lastBodyDoneNs     atomic.Int64

	dispatchLagHist   *durationHistogram
	queueWaitHist     *durationHistogram
	timeToHeadersHist *durationHistogram
	bodyReadHist      *durationHistogram
	slotHoldHist      *durationHistogram
	connectHist       *durationHistogram

	mu           sync.Mutex
	statusCounts map[string]uint64
	errorCounts  map[string]uint64
	perTarget    map[string]*perTargetMetrics
	phaseTraces  []phaseTraceRecord
}

func newMetricsCollector(workerID string, maxActive int, queueCapacity int, maxConnsPerHost int, enableHTTPTrace bool) *metricsCollector {
	return &metricsCollector{
		workerID:          workerID,
		maxActive:         maxActive,
		queueCapacity:     queueCapacity,
		maxConnsPerHost:   maxConnsPerHost,
		enableHTTPTrace:   enableHTTPTrace,
		startedAt:         nowSeconds(),
		dispatchLagHist:   newDurationHistogram(defaultHistogramBounds),
		queueWaitHist:     newDurationHistogram(defaultHistogramBounds),
		timeToHeadersHist: newDurationHistogram(defaultHistogramBounds),
		bodyReadHist:      newDurationHistogram(defaultHistogramBounds),
		slotHoldHist:      newDurationHistogram(defaultHistogramBounds),
		connectHist:       newDurationHistogram(defaultHistogramBounds),
		statusCounts:      make(map[string]uint64),
		errorCounts:       make(map[string]uint64),
		perTarget:         make(map[string]*perTargetMetrics),
	}
}

func nowSeconds() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func recordMax(target *atomic.Int64, value int64) {
	for {
		current := target.Load()
		if value <= current {
			return
		}
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

func recordMaxTime(target *atomic.Int64, at time.Time) {
	if at.IsZero() {
		return
	}
	recordMax(target, at.UnixNano())
}

func (c *metricsCollector) IncActive() {
	value := c.activeRequests.Add(1)
	recordMax(&c.maxObservedActive, value)
}

func (c *metricsCollector) DecActive() {
	c.activeRequests.Add(-1)
}

func (c *metricsCollector) IncOutstanding() {
	value := c.outstandingRequests.Add(1)
	recordMax(&c.maxObservedOutstanding, value)
}

func (c *metricsCollector) DecOutstanding() {
	c.outstandingRequests.Add(-1)
}

func (c *metricsCollector) ObserveQueueDepth(depth int) {
	recordMax(&c.maxObservedQueueDepth, int64(depth))
}

func (c *metricsCollector) ObserveDispatchLag(d time.Duration) {
	c.dispatchLagHist.Observe(d)
}

func (c *metricsCollector) ObserveQueueWait(d time.Duration) {
	c.queueWaitHist.Observe(d)
}

func (c *metricsCollector) ObserveTimeToHeaders(d time.Duration) {
	c.timeToHeadersHist.Observe(d)
}

func (c *metricsCollector) ObserveBodyRead(d time.Duration) {
	c.bodyReadHist.Observe(d)
}

func (c *metricsCollector) ObserveSlotHold(d time.Duration) {
	c.slotHoldHist.Observe(d)
}

func (c *metricsCollector) ObserveConnect(d time.Duration) {
	c.connectHist.Observe(d)
}

func (c *metricsCollector) recordConnection(target string, state httpTraceState) {
	if state.newConnection {
		c.newConnections.Add(1)
	}
	if state.reused {
		c.reusedConnections.Add(1)
	}
	if state.wasIdle {
		c.reusedIdleConnections.Add(1)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	targetMetrics := c.ensureTarget(target)
	if state.newConnection {
		targetMetrics.NewConnections++
	}
	if state.reused {
		targetMetrics.ReusedConnections++
	}
	if state.wasIdle {
		targetMetrics.ReusedIdleConnections++
	}
}

func (c *metricsCollector) ensureTarget(target string) *perTargetMetrics {
	targetMetrics := c.perTarget[target]
	if targetMetrics == nil {
		targetMetrics = &perTargetMetrics{
			StatusCounts: make(map[string]uint64),
			ErrorCounts:  make(map[string]uint64),
		}
		c.perTarget[target] = targetMetrics
	}
	return targetMetrics
}

func (c *metricsCollector) RecordCompletion(target string, rec resultRecord, sampledTrace *phaseTraceRecord) {
	c.completedRequests.Add(1)
	if rec.Success {
		c.succeededRequests.Add(1)
	} else {
		c.failedRequests.Add(1)
	}
	recordMaxTime(&c.lastRequestStartNs, unixFloatSecondsToTime(rec.RequestStartAt))
	recordMaxTime(&c.lastBodyDoneNs, unixFloatSecondsToTime(rec.BodyDoneAt))

	statusKey := "none"
	if rec.StatusCode > 0 {
		statusKey = fmt.Sprintf("%d", rec.StatusCode)
	}
	errorKey := rec.ErrorClass
	if errorKey == "" {
		errorKey = "none"
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.statusCounts[statusKey]++
	c.errorCounts[errorKey]++
	targetMetrics := c.ensureTarget(target)
	targetMetrics.Requests++
	if rec.Success {
		targetMetrics.Successes++
	} else {
		targetMetrics.Failures++
	}
	targetMetrics.StatusCounts[statusKey]++
	targetMetrics.ErrorCounts[errorKey]++
	if sampledTrace != nil {
		c.phaseTraces = append(c.phaseTraces, *sampledTrace)
	}
}

func (c *metricsCollector) Snapshot(runT0 float64, requestsLoaded int, requestsScheduled int) goClientMetrics {
	c.mu.Lock()
	defer c.mu.Unlock()

	perTarget := make(map[string]perTargetMetrics, len(c.perTarget))
	for key, value := range c.perTarget {
		perTarget[key] = perTargetMetrics{
			Requests:              value.Requests,
			Successes:             value.Successes,
			Failures:              value.Failures,
			NewConnections:        value.NewConnections,
			ReusedConnections:     value.ReusedConnections,
			ReusedIdleConnections: value.ReusedIdleConnections,
			StatusCounts:          cloneCountMap(value.StatusCounts),
			ErrorCounts:           cloneCountMap(value.ErrorCounts),
		}
	}

	return goClientMetrics{
		SchemaVersion:          "clientlab.go_metrics.v1",
		WorkerID:               c.workerID,
		RunT0:                  runT0,
		StartedAt:              c.startedAt,
		CompletedAt:            nowSeconds(),
		RequestsLoaded:         requestsLoaded,
		RequestsScheduled:      requestsScheduled,
		RequestsCompleted:      c.completedRequests.Load(),
		RequestsSucceeded:      c.succeededRequests.Load(),
		RequestsFailed:         c.failedRequests.Load(),
		EnableHTTPTrace:        c.enableHTTPTrace,
		MaxActiveRequests:      c.maxActive,
		QueueCapacity:          c.queueCapacity,
		MaxConnsPerHost:        c.maxConnsPerHost,
		MaxObservedActive:      c.maxObservedActive.Load(),
		MaxObservedOutstanding: c.maxObservedOutstanding.Load(),
		MaxObservedQueueDepth:  c.maxObservedQueueDepth.Load(),
		NewConnections:         c.newConnections.Load(),
		ReusedConnections:      c.reusedConnections.Load(),
		ReusedIdleConnections:  c.reusedIdleConnections.Load(),
		LastRequestStartAt:     float64(c.lastRequestStartNs.Load()) / 1e9,
		LastBodyDoneAt:         float64(c.lastBodyDoneNs.Load()) / 1e9,
		StatusCounts:           cloneCountMap(c.statusCounts),
		ErrorCounts:            cloneCountMap(c.errorCounts),
		Histograms: map[string]histogramSnapshot{
			"dispatch_lag":    c.dispatchLagHist.Snapshot(),
			"queue_wait":      c.queueWaitHist.Snapshot(),
			"time_to_headers": c.timeToHeadersHist.Snapshot(),
			"body_read":       c.bodyReadHist.Snapshot(),
			"slot_hold":       c.slotHoldHist.Snapshot(),
			"connect":         c.connectHist.Snapshot(),
		},
		PerTarget: perTarget,
	}
}

func (c *metricsCollector) WriteMetrics(path string, runT0 float64, requestsLoaded int, requestsScheduled int) error {
	if path == "" {
		return nil
	}
	payload := c.Snapshot(runT0, requestsLoaded, requestsScheduled)
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(path, data)
}

func (c *metricsCollector) WritePhaseTraces(path string) error {
	if path == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	lines := make([]byte, 0, len(c.phaseTraces)*128)
	for _, trace := range c.phaseTraces {
		row, err := json.Marshal(trace)
		if err != nil {
			return err
		}
		lines = append(lines, row...)
		lines = append(lines, '\n')
	}
	return writeFile(path, lines)
}

func cloneCountMap(input map[string]uint64) map[string]uint64 {
	output := make(map[string]uint64, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func unixFloatSecondsToTime(value float64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	seconds := int64(value)
	nanos := int64((value - float64(seconds)) * 1e9)
	return time.Unix(seconds, nanos)
}

func buildClientTrace(enabled bool, target string, collector *metricsCollector, state *httpTraceState) *httptrace.ClientTrace {
	if !enabled || state == nil || collector == nil {
		return nil
	}
	return &httptrace.ClientTrace{
		ConnectStart: func(_, _ string) {
			state.connectStartAt = time.Now()
		},
		ConnectDone: func(_, _ string, err error) {
			if err == nil {
				state.connectDoneAt = time.Now()
				if !state.connectStartAt.IsZero() {
					collector.ObserveConnect(state.connectDoneAt.Sub(state.connectStartAt))
				}
			}
		},
		GotConn: func(info httptrace.GotConnInfo) {
			state.reused = info.Reused
			state.wasIdle = info.WasIdle
			state.newConnection = !info.Reused
			collector.recordConnection(target, *state)
		},
		GotFirstResponseByte: func() {
			state.headersAt = time.Now()
		},
	}
}

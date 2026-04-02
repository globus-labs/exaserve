package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// SaturationConfig holds all parameters for the saturation finder.
type SaturationConfig struct {
	BaseURLs        []string
	Model           string
	PromptWords     int
	OutputTokens    int
	MaxActive       int
	MaxConnsPerHost int
	NumGoWorkers    int
	TimeoutSec      float64
	EnableHTTPTrace bool

	// Search parameters
	SearchMode     string  // "binary" or "step-up"
	InitialRate    int     // starting rps for binary search low bound
	MaxRate        int     // upper bound hint (0 = auto-detect)
	StepDuration   float64 // measurement window in seconds
	WarmupDuration float64 // warmup before measurement in seconds
	CooldownPause  float64 // pause between steps in seconds
	Tolerance      float64 // convergence: (hi-lo)/hi < tolerance

	// SLO thresholds
	MaxErrorRate float64 // max fraction of errors
	PlateauRatio float64 // min achieved/target ratio
	MaxP99TTFT   float64 // max acceptable p99 TTFT in seconds (0 = disabled)

	// Streaming
	Stream bool // enable streaming responses for TTFT measurement

	// Step-up parameters
	StepUpStart     int
	StepUpEnd       int
	StepUpIncrement int
	Verify          bool // run verification after binary search

	// Output
	OutputFile string
}

func (cfg SaturationConfig) Validate() error {
	if len(cfg.BaseURLs) == 0 {
		return errors.New("at least one base URL is required")
	}
	if cfg.MaxActive < 1 {
		return errors.New("max active requests must be >= 1")
	}
	if cfg.NumGoWorkers < 1 {
		return errors.New("num go workers must be >= 1")
	}
	if cfg.TimeoutSec <= 0 {
		return errors.New("timeout must be > 0")
	}
	if cfg.StepDuration <= 0 {
		return errors.New("step duration must be > 0")
	}
	if cfg.WarmupDuration < 0 {
		return errors.New("warmup duration must be >= 0")
	}
	if cfg.CooldownPause < 0 {
		return errors.New("cooldown pause must be >= 0")
	}
	if cfg.Tolerance <= 0 || cfg.Tolerance >= 1 {
		return errors.New("tolerance must be in (0, 1)")
	}
	if cfg.MaxErrorRate < 0 || cfg.MaxErrorRate > 1 {
		return errors.New("max error rate must be in [0, 1]")
	}
	if cfg.PlateauRatio <= 0 || cfg.PlateauRatio > 1 {
		return errors.New("plateau ratio must be in (0, 1]")
	}
	switch cfg.SearchMode {
	case "binary":
		if cfg.InitialRate <= 0 {
			return errors.New("initial rate must be > 0")
		}
		if cfg.MaxRate < 0 {
			return errors.New("max rate must be >= 0")
		}
	case "step-up":
		if cfg.StepUpStart <= 0 || cfg.StepUpEnd <= 0 || cfg.StepUpIncrement <= 0 {
			return errors.New("step-up requires positive start, end, and increment")
		}
		if cfg.StepUpEnd < cfg.StepUpStart {
			return errors.New("step-up end must be >= start")
		}
	default:
		return fmt.Errorf("unknown search mode %q", cfg.SearchMode)
	}
	return nil
}

// SaturationOutput is the JSON structure written to the output file.
type SaturationOutput struct {
	Mode              string        `json:"mode"`
	SaturationRate    int           `json:"saturation_rate"`
	Tolerance         float64       `json:"tolerance"`
	SLO               sloConfig     `json:"slo"`
	Steps             []*StepResult `json:"steps"`
	VerificationSteps []*StepResult `json:"verification_steps,omitempty"`
}

type sloConfig struct {
	MaxErrorRate float64 `json:"max_error_rate"`
	PlateauRatio float64 `json:"plateau_ratio"`
}

type saturationFinder struct {
	cfg       SaturationConfig
	gen       *SynthGenerator
	clients   []*http.Client // per-scheduler clients with separate transports
	ctx       context.Context
	cancel    context.CancelFunc
	logPrefix string
}

func runSaturation(cfg SaturationConfig) int {
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "[sat] ERROR: %v\n", err)
		return 1
	}
	sf := newSaturationFinder(cfg)
	defer sf.cancel()

	fmt.Fprintf(os.Stderr, "%s Starting saturation finder (mode=%s)\n", sf.logPrefix, cfg.SearchMode)

	var steps []*StepResult
	var verificationSteps []*StepResult
	saturationRate := 0

	switch cfg.SearchMode {
	case "binary":
		steps, saturationRate = sf.binarySearch()
		if saturationRate > 0 && cfg.Verify {
			verificationSteps = sf.stepUpVerify(saturationRate)
		}
	case "step-up":
		var err error
		steps, err = sf.stepUp()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s ERROR: %v\n", sf.logPrefix, err)
			return 1
		}
		// Find the last healthy step as the saturation point
		for i := len(steps) - 1; i >= 0; i-- {
			if steps[i].Healthy {
				saturationRate = steps[i].TargetRate
				break
			}
		}
	default:
		fmt.Fprintf(os.Stderr, "%s ERROR: unknown search mode %q\n", sf.logPrefix, cfg.SearchMode)
		return 1
	}

	output := SaturationOutput{
		Mode:           cfg.SearchMode,
		SaturationRate: saturationRate,
		Tolerance:      cfg.Tolerance,
		SLO:            sloConfig{MaxErrorRate: cfg.MaxErrorRate, PlateauRatio: cfg.PlateauRatio},
		Steps:          steps,
	}
	if len(verificationSteps) > 0 {
		output.VerificationSteps = verificationSteps
	}

	if err := writeOutputJSON(cfg.OutputFile, output); err != nil {
		fmt.Fprintf(os.Stderr, "%s ERROR: could not write output: %v\n", sf.logPrefix, err)
		return 1
	}

	fmt.Fprintf(os.Stderr, "%s Saturation point: %d rps (%d search steps, %d verification steps)\n",
		sf.logPrefix, saturationRate, len(steps), len(verificationSteps))
	return 0
}

func runSaturationStep(cfg SaturationConfig, targetRate int) int {
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "[sat] ERROR: %v\n", err)
		return 1
	}
	sf := newSaturationFinder(cfg)
	defer sf.cancel()

	fmt.Fprintf(os.Stderr, "%s Running single step at %d rps\n", sf.logPrefix, targetRate)

	result := sf.runStep(targetRate)
	sf.evaluateHealth(result)

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s ERROR: marshal step result: %v\n", sf.logPrefix, err)
		return 1
	}
	if err := writeFile(cfg.OutputFile, data); err != nil {
		fmt.Fprintf(os.Stderr, "%s ERROR: write step result: %v\n", sf.logPrefix, err)
		return 1
	}

	fmt.Fprintf(os.Stderr, "%s Step done: target=%d achieved=%.1f healthy=%v\n",
		sf.logPrefix, result.TargetRate, result.AchievedRate, result.Healthy)
	return 0
}

func newSaturationFinder(cfg SaturationConfig) *saturationFinder {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintf(os.Stderr, "[sat] Interrupt received, stopping...\n")
		cancel()
	}()

	numSchedulers := cfg.NumGoWorkers
	if numSchedulers < 1 {
		numSchedulers = 1
	}
	maxConns := cfg.MaxConnsPerHost
	if maxConns <= 0 {
		maxConns = cfg.MaxActive
	}
	perClientIdle := ceilDiv(cfg.MaxActive, numSchedulers)
	perClientConns := ceilDiv(maxConns, numSchedulers)
	if perClientConns < 1 {
		perClientConns = 1
	}

	// Per-scheduler transports avoid mutex contention in http.Transport's
	// connection pool, matching the replay mode's approach.
	clients := make([]*http.Client, numSchedulers)
	for i := 0; i < numSchedulers; i++ {
		t := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          perClientIdle,
			MaxIdleConnsPerHost:   perClientIdle,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableKeepAlives:     false,
			ForceAttemptHTTP2:     false,
		}
		if maxConns > 0 {
			t.MaxConnsPerHost = perClientConns
		}
		clients[i] = &http.Client{
			Transport: t,
			Timeout:   time.Duration(cfg.TimeoutSec * float64(time.Second)),
		}
	}

	return &saturationFinder{
		cfg:       cfg,
		gen:       NewSynthGenerator(cfg.Model, cfg.PromptWords, cfg.OutputTokens, cfg.Stream),
		clients:   clients,
		ctx:       ctx,
		cancel:    cancel,
		logPrefix: "[sat]",
	}
}

// binarySearch finds the saturation rate via ceiling probe + binary search.
func (sf *saturationFinder) binarySearch() ([]*StepResult, int) {
	var steps []*StepResult

	lo := sf.cfg.InitialRate
	hi := sf.cfg.MaxRate

	// Phase 1: Ceiling probe if no upper bound given.
	if hi <= 0 {
		fmt.Fprintf(os.Stderr, "%s Ceiling probe starting at %d rps\n", sf.logPrefix, lo)
		rate := lo
		var lastHealthy int
		for {
			if sf.ctx.Err() != nil {
				break
			}
			result := sf.runStep(rate)
			sf.evaluateHealth(result)
			steps = append(steps, result)
			fmt.Fprintf(os.Stderr, "%s   probe %d rps → achieved=%.1f healthy=%v\n",
				sf.logPrefix, rate, result.AchievedRate, result.Healthy)
			if !result.Healthy {
				hi = rate
				lo = lastHealthy
				if lo == 0 {
					lo = rate / 2
				}
				break
			}
			lastHealthy = rate
			rate *= 2
		}
		if hi <= 0 {
			// All probes were healthy; last tested rate is the best we found.
			return steps, lastHealthy
		}
	}

	// Phase 2: Binary search between lo and hi.
	fmt.Fprintf(os.Stderr, "%s Binary search: lo=%d hi=%d tolerance=%.3f\n",
		sf.logPrefix, lo, hi, sf.cfg.Tolerance)

	for {
		if sf.ctx.Err() != nil {
			break
		}
		if hi <= 0 || (float64(hi-lo)/float64(hi)) <= sf.cfg.Tolerance {
			break
		}
		mid := (lo + hi) / 2
		if mid == lo {
			break
		}
		result := sf.runStep(mid)
		sf.evaluateHealth(result)
		steps = append(steps, result)
		fmt.Fprintf(os.Stderr, "%s   search [%d, %d] → %d rps: achieved=%.1f healthy=%v\n",
			sf.logPrefix, lo, hi, mid, result.AchievedRate, result.Healthy)
		if result.Healthy {
			lo = mid
		} else {
			hi = mid
		}
	}

	return steps, lo
}

// stepUp runs a linear sweep of rates.
func (sf *saturationFinder) stepUp() ([]*StepResult, error) {
	start := sf.cfg.StepUpStart
	end := sf.cfg.StepUpEnd
	inc := sf.cfg.StepUpIncrement
	if start <= 0 || end <= 0 || inc <= 0 {
		return nil, errors.New("step-up requires --sat-step-up-start, --sat-step-up-end, --sat-step-up-increment")
	}

	var steps []*StepResult
	for rate := start; rate <= end; rate += inc {
		if sf.ctx.Err() != nil {
			break
		}
		result := sf.runStep(rate)
		sf.evaluateHealth(result)
		steps = append(steps, result)
		fmt.Fprintf(os.Stderr, "%s   step-up %d rps: achieved=%.1f healthy=%v\n",
			sf.logPrefix, rate, result.AchievedRate, result.Healthy)
	}
	return steps, nil
}

// stepUpVerify runs fine-grained steps around the found saturation point.
func (sf *saturationFinder) stepUpVerify(satRate int) []*StepResult {
	start := int(float64(satRate) * 0.7)
	end := int(float64(satRate) * 1.3)
	inc := int(float64(satRate) * 0.05)
	if inc < 1 {
		inc = 1
	}
	if start < 1 {
		start = 1
	}

	fmt.Fprintf(os.Stderr, "%s Verification: %d to %d rps (step %d)\n",
		sf.logPrefix, start, end, inc)

	var steps []*StepResult
	for rate := start; rate <= end; rate += inc {
		if sf.ctx.Err() != nil {
			break
		}
		result := sf.runStep(rate)
		sf.evaluateHealth(result)
		steps = append(steps, result)
		fmt.Fprintf(os.Stderr, "%s   verify %d rps: achieved=%.1f healthy=%v\n",
			sf.logPrefix, rate, result.AchievedRate, result.Healthy)
	}
	return steps
}

// runStep dispatches requests at targetRate for the configured duration and
// returns a StepResult with measured metrics.
func (sf *saturationFinder) runStep(targetRate int) *StepResult {
	if targetRate <= 0 {
		return &StepResult{TargetRate: targetRate, Healthy: false, FailReasons: []string{"target_rate <= 0"}}
	}

	// Cooldown between steps.
	if sf.cfg.CooldownPause > 0 {
		select {
		case <-time.After(time.Duration(sf.cfg.CooldownPause * float64(time.Second))):
		case <-sf.ctx.Done():
			return &StepResult{TargetRate: targetRate, Healthy: false, FailReasons: []string{"cancelled"}}
		}
	}

	numSchedulers := len(sf.clients)
	maxActive := sf.cfg.MaxActive
	maxConns := sf.cfg.MaxConnsPerHost
	if maxConns <= 0 {
		maxConns = maxActive
	}

	// Create collectors: nil for warmup (discarded), real for measurement.
	measureCollector := newMetricsCollector("sat", maxActive, 0, maxConns, sf.cfg.EnableHTTPTrace)

	workCh := make(chan workItem, maxActive)
	outstandingSlots := make(chan struct{}, maxActive)
	var poolWg sync.WaitGroup

	for i := 0; i < maxActive; i++ {
		clientIdx := i % numSchedulers
		poolWg.Add(1)
		go func(client *http.Client) {
			defer poolWg.Done()
			for item := range workCh {
				_ = doRequest(sf.ctx, client, item, item.collector)
				<-outstandingSlots
				if item.collector != nil {
					item.collector.DecOutstanding()
				}
			}
		}(sf.clients[clientIdx])
	}

	// Dispatch goroutines: spread targetRate across numSchedulers.
	perSchedulerRate := targetRate / numSchedulers
	remainder := targetRate % numSchedulers

	var measureStartNs atomic.Int64
	var dispatchWg sync.WaitGroup

	for wIdx := 0; wIdx < numSchedulers; wIdx++ {
		myRate := perSchedulerRate
		if wIdx < remainder {
			myRate++
		}
		if myRate <= 0 {
			continue
		}
		myWarmupCount := int(float64(myRate) * sf.cfg.WarmupDuration)
		myMeasureCount := int(float64(myRate) * sf.cfg.StepDuration)

		dispatchWg.Add(1)
		go func(schedulerID int, rate int, warmupCount int, measureCount int) {
			defer dispatchWg.Done()
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()

			interval := time.Second / time.Duration(rate)
			sleepTimer := time.NewTimer(0)
			if !sleepTimer.Stop() {
				<-sleepTimer.C
			}

			baseTime := time.Now()
			totalToSend := warmupCount + measureCount
			var localURLIdx uint64

			for i := 0; i < totalToSend; i++ {
				if sf.ctx.Err() != nil {
					return
				}

				targetTime := baseTime.Add(time.Duration(i) * interval)
				if remaining := targetTime.Sub(time.Now()); remaining > time.Millisecond {
					coarse := remaining - 500*time.Microsecond
					sleepTimer.Reset(coarse)
					select {
					case <-sleepTimer.C:
					case <-sf.ctx.Done():
						if !sleepTimer.Stop() {
							<-sleepTimer.C
						}
						return
					}
				}
				for time.Now().Before(targetTime) {
					if sf.ctx.Err() != nil {
						return
					}
				}

				// Determine collector: nil during warmup, real during measurement.
				var collector *metricsCollector
				if i >= warmupCount {
					collector = measureCollector
					// Record the start of measurement phase (first writer wins).
					measureStartNs.CompareAndSwap(0, time.Now().UnixNano())
				}

				select {
				case outstandingSlots <- struct{}{}:
					if collector != nil {
						collector.IncOutstanding()
					}
				case <-sf.ctx.Done():
					return
				}

				req := sf.gen.Next()
				target := sf.cfg.BaseURLs[localURLIdx%uint64(len(sf.cfg.BaseURLs))]
				localURLIdx++

				samplePhases := sf.cfg.EnableHTTPTrace && rand.Float64() <= 0.01

				item := workItem{
					req:             req,
					target:          target,
					scheduledAt:     float64(targetTime.UnixNano()) / 1e9,
					enqueuedAt:      nowSeconds(),
					workerID:        schedulerID,
					resultIndex:     0,
					samplePhases:    samplePhases,
					enableConnTrace: sf.cfg.EnableHTTPTrace,
					stream:          sf.cfg.Stream,
					collector:       collector,
				}
				workCh <- item
			}
		}(wIdx, myRate, myWarmupCount, myMeasureCount)
	}

	dispatchWg.Wait()
	close(workCh)
	poolWg.Wait()

	return SnapshotStepResult(measureCollector, targetRate, measureStartNs.Load(), sf.cfg.StepDuration)
}

func (sf *saturationFinder) evaluateHealth(r *StepResult) {
	r.Healthy = true
	r.FailReasons = nil

	if r.ErrorRate > sf.cfg.MaxErrorRate {
		r.Healthy = false
		r.FailReasons = append(r.FailReasons,
			fmt.Sprintf("error_rate=%.4f > %.4f", r.ErrorRate, sf.cfg.MaxErrorRate))
	}

	if r.TargetRate > 0 {
		ratio := r.AchievedRate / float64(r.TargetRate)
		if ratio < sf.cfg.PlateauRatio {
			r.Healthy = false
			r.FailReasons = append(r.FailReasons,
				fmt.Sprintf("plateau: achieved/target=%.4f < %.4f", ratio, sf.cfg.PlateauRatio))
		}
	}

	if sf.cfg.MaxP99TTFT > 0 && r.P99TTFT > sf.cfg.MaxP99TTFT {
		r.Healthy = false
		r.FailReasons = append(r.FailReasons,
			fmt.Sprintf("ttft: p99=%.4fs > %.4fs", r.P99TTFT, sf.cfg.MaxP99TTFT))
	}
}

func writeOutputJSON(path string, output SaturationOutput) error {
	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(path, data)
}

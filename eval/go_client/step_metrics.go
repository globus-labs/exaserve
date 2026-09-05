package main

import "math"

// StepResult holds the outcome of a single saturation measurement step.
type StepResult struct {
	TargetRate       int                `json:"target_rate"`
	AchievedRate     float64            `json:"achieved_rate"`
	Duration         float64            `json:"duration_s"`
	Completed        uint64             `json:"completed"`
	Failed           uint64             `json:"failed"`
	ErrorRate        float64            `json:"error_rate"`
	P50Latency       float64            `json:"p50_latency_s"`
	P99Latency       float64            `json:"p99_latency_s"`
	MeanLatency      float64            `json:"mean_latency_s"`
	LatencyHistogram histogramSnapshot  `json:"latency_histogram"`
	P50TTFT          float64            `json:"p50_ttft_s,omitempty"`
	P99TTFT          float64            `json:"p99_ttft_s,omitempty"`
	MeanTTFT         float64            `json:"mean_ttft_s,omitempty"`
	TTFTHistogram    *histogramSnapshot `json:"ttft_histogram,omitempty"`
	NewConns         uint64             `json:"new_connections"`
	ReusedConns      uint64             `json:"reused_connections"`
	MaxActive        int64              `json:"max_observed_active"`
	Healthy          bool               `json:"healthy"`
	FailReasons      []string           `json:"fail_reasons,omitempty"`
}

// SnapshotStepResult reads atomic counters from a metricsCollector and computes
// derived fields (achieved rate, error rate, latency percentiles).
// measureStartNs is the wall-clock nanosecond timestamp when measurement began.
// If 0, falls back to the provided fallbackDuration.
func SnapshotStepResult(mc *metricsCollector, targetRate int, measureStartNs int64, fallbackDuration float64) *StepResult {
	// completedRequests includes both successes and failures.
	completed := mc.completedRequests.Load()
	succeeded := mc.succeededRequests.Load()
	failed := mc.failedRequests.Load()

	var errorRate float64
	if completed > 0 {
		errorRate = float64(failed) / float64(completed)
	}

	// Duration: from measureStart to the last recorded body-done timestamp.
	// This excludes drain time after the last response arrives.
	lastBodyDoneNs := mc.lastBodyDoneNs.Load()
	var duration float64
	if measureStartNs > 0 && lastBodyDoneNs > measureStartNs {
		duration = float64(lastBodyDoneNs-measureStartNs) / 1e9
	} else {
		duration = fallbackDuration
	}

	var achievedRate float64
	if duration > 0 {
		achievedRate = float64(succeeded) / duration
	}

	tthSnap := mc.timeToHeadersHist.Snapshot()
	ttftSnap := mc.ttftHist.Snapshot()

	result := &StepResult{
		TargetRate:       targetRate,
		AchievedRate:     achievedRate,
		Duration:         duration,
		Completed:        succeeded,
		Failed:           failed,
		ErrorRate:        errorRate,
		P50Latency:       PercentileFromHistogram(&tthSnap, 0.50),
		P99Latency:       PercentileFromHistogram(&tthSnap, 0.99),
		MeanLatency:      histogramMeanS(&tthSnap),
		LatencyHistogram: tthSnap,
		NewConns:         mc.newConnections.Load(),
		ReusedConns:      mc.reusedConnections.Load(),
		MaxActive:        mc.maxObservedActive.Load(),
		Healthy:          true, // caller evaluates SLO
	}
	// Populate TTFT fields only when streaming data is available.
	if ttftSnap.Count > 0 {
		result.P50TTFT = PercentileFromHistogram(&ttftSnap, 0.50)
		result.P99TTFT = PercentileFromHistogram(&ttftSnap, 0.99)
		result.MeanTTFT = histogramMeanS(&ttftSnap)
		result.TTFTHistogram = &ttftSnap
	}
	return result
}

// PercentileFromHistogram estimates a percentile from a bucket-based histogram
// snapshot using linear interpolation within the target bucket.
func PercentileFromHistogram(snap *histogramSnapshot, p float64) float64 {
	if snap.Count == 0 {
		return 0.0
	}
	threshold := uint64(math.Ceil(float64(snap.Count) * p))
	if threshold == 0 {
		threshold = 1
	}

	var cumulative uint64
	for i, count := range snap.Counts {
		cumulative += count
		if cumulative >= threshold {
			upper := snap.BucketUpperBoundsS[i]
			// The overflow bucket has no finite upper bound. Returning the global
			// mean can understate a tail percentile by orders of magnitude, so use
			// the final finite boundary as a conservative lower bound.
			if upper < 0 {
				if i > 0 {
					return snap.BucketUpperBoundsS[i-1]
				}
				return 0.0
			}
			// Determine lower bound of this bucket.
			var lower float64
			if i > 0 {
				lower = snap.BucketUpperBoundsS[i-1]
			}
			// Linear interpolation within bucket.
			prevCumulative := cumulative - count
			if count == 0 {
				return upper
			}
			fraction := float64(threshold-prevCumulative) / float64(count)
			return lower + fraction*(upper-lower)
		}
	}
	// A malformed/incomplete bucket vector must not understate the tail with
	// the global mean. Return the greatest finite lower bound available.
	for i := len(snap.BucketUpperBoundsS) - 1; i >= 0; i-- {
		if snap.BucketUpperBoundsS[i] >= 0 {
			return snap.BucketUpperBoundsS[i]
		}
	}
	return 0.0
}

func histogramMeanS(snap *histogramSnapshot) float64 {
	if snap.Count == 0 {
		return 0.0
	}
	return snap.SumS / float64(snap.Count)
}

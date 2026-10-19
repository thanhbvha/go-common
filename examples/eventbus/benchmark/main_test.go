package main

import (
	"context"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/examples/eventbus/internal/demo"
)

func TestValidateMatrixDimensions(t *testing.T) {
	valid := config{Config: demo.DefaultConfig(), PayloadKB: 1, Concurrency: 10, Workload: "retry10", Messages: 1, Timeout: time.Second, Delay: time.Second, SampleEvery: time.Second}
	if err := validate(valid); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	tests := []config{
		func() config { c := valid; c.PayloadKB = 2; return c }(),
		func() config { c := valid; c.Concurrency = 2; return c }(),
		func() config { c := valid; c.Workload = "unknown"; return c }(),
		func() config { c := valid; c.Messages = 0; return c }(),
	}
	for _, cfg := range tests {
		if err := validate(cfg); err == nil {
			t.Fatalf("invalid config accepted: %#v", cfg)
		}
	}
}

func TestRetryMix(t *testing.T) {
	var retry10, retry50 int
	for i := range 100 {
		if shouldRetry("retry10", i) {
			retry10++
		}
		if shouldRetry("retry50", i) {
			retry50++
		}
	}
	if retry10 != 10 || retry50 != 50 {
		t.Fatalf("retry counts = %d/%d, want 10/50", retry10, retry50)
	}
}

func TestLatencyHistogramIsBoundedAndEstimatesPercentiles(t *testing.T) {
	var histogram latencyHistogram
	bucketCount := len(histogram.buckets)
	if bucketCount != latencyHistogramBuckets {
		t.Fatalf("bucket count = %d, want %d", bucketCount, latencyHistogramBuckets)
	}
	for range 1_000_000 {
		histogram.Observe(time.Millisecond)
	}
	if got := histogram.count.Load(); got != 1_000_000 {
		t.Fatalf("count = %d, want 1000000", got)
	}
	if got := histogram.Percentile(0.99); got < 1 || got > 1.1 {
		t.Fatalf("p99 = %vms, want bounded estimate near 1ms", got)
	}
}

func TestCompletionTrackerCountsUniqueSequences(t *testing.T) {
	tracker := newCompletionTracker(1_000_000)
	if words := len(tracker.seen); words != 15_625 {
		t.Fatalf("bitmap words = %d, want 15625", words)
	}
	if !tracker.Mark(42) {
		t.Fatal("first completion was rejected")
	}
	if tracker.Mark(42) {
		t.Fatal("duplicate completion was accepted")
	}
	if got := tracker.duplicates.Load(); got != 1 {
		t.Fatalf("duplicates = %d, want 1", got)
	}
}

func TestLatencyHistogramClampsRange(t *testing.T) {
	if got := latencyBucket(0); got != 0 {
		t.Fatalf("zero bucket = %d, want 0", got)
	}
	if got := latencyBucket(latencyHistogramMax * 2); got != latencyHistogramBuckets-1 {
		t.Fatalf("overflow bucket = %d, want %d", got, latencyHistogramBuckets-1)
	}
}

func TestCompletionCounterNotificationIsBounded(t *testing.T) {
	completed := newCompletionCounter()
	for range 1_000_000 {
		completed.Add()
	}
	if got := cap(completed.notify); got != 1 {
		t.Fatalf("notification capacity = %d, want 1", got)
	}
	if got := completed.count.Load(); got != 1_000_000 {
		t.Fatalf("completion count = %d, want 1000000", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := completed.Wait(ctx, 1_000_000); err != nil {
		t.Fatalf("wait: %v", err)
	}
}

func TestValidateMeasurementConfig(t *testing.T) {
	valid := config{
		Config: demo.DefaultConfig(), PayloadKB: 1, Concurrency: 10,
		Workload: "delayed", Messages: 1, Timeout: time.Second,
		Delay: time.Second, SampleEvery: time.Second,
	}
	for name, mutate := range map[string]func(*config){
		"delay":        func(cfg *config) { cfg.Delay = 0 },
		"sample every": func(cfg *config) { cfg.SampleEvery = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			mutate(&cfg)
			if err := validate(cfg); err == nil {
				t.Fatalf("invalid config accepted: %#v", cfg)
			}
		})
	}
}

func TestValidateDurationMode(t *testing.T) {
	valid := config{
		Config: demo.DefaultConfig(), PayloadKB: 1, Concurrency: 10,
		Workload: "immediate", Messages: 1, Duration: time.Minute, Rate: 100,
		Timeout: 2 * time.Minute, Delay: time.Second, SampleEvery: time.Second,
	}
	if err := validate(valid); err != nil {
		t.Fatalf("valid duration config: %v", err)
	}
	if got, err := plannedMessages(valid); err != nil || got != 6000 {
		t.Fatalf("planned messages = %d, %v; want 6000", got, err)
	}
	for name, mutate := range map[string]func(*config){
		"missing rate":       func(cfg *config) { cfg.Rate = 0 },
		"short timeout":      func(cfg *config) { cfg.Timeout = cfg.Duration },
		"non-kafka worker":   func(cfg *config) { cfg.SecondWorkerAfter = time.Second },
		"preloaded worker":   func(cfg *config) { cfg.Driver = "kafka"; cfg.Preload = true; cfg.SecondWorkerAfter = time.Second },
		"unbounded tracking": func(cfg *config) { cfg.Duration = 2 * time.Hour; cfg.Rate = 1_000_000; cfg.Timeout = 3 * time.Hour },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			mutate(&cfg)
			if err := validate(cfg); err == nil {
				t.Fatalf("invalid duration config accepted: %#v", cfg)
			}
		})
	}
}

func TestValidateKafkaSecondWorker(t *testing.T) {
	cfg := config{
		Config: demo.DefaultConfig(), PayloadKB: 1, Concurrency: 10,
		Workload: "immediate", Messages: 100, Timeout: time.Minute,
		Delay: time.Second, SampleEvery: time.Second, SecondWorkerAfter: time.Second,
	}
	cfg.Driver = "kafka"
	if err := validate(cfg); err != nil {
		t.Fatalf("valid Kafka second worker: %v", err)
	}
}

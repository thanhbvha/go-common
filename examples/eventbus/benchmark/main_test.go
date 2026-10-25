package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
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

func TestExpectedRetries(t *testing.T) {
	if got := expectedRetries("immediate", 100); got != 0 {
		t.Fatalf("immediate retries = %d, want 0", got)
	}
	if got := expectedRetries("retry10", 100); got != 10 {
		t.Fatalf("retry10 retries = %d, want 10", got)
	}
	if got := expectedRetries("retry50", 101); got != 51 {
		t.Fatalf("retry50 retries = %d, want 51", got)
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
		"late second worker": func(cfg *config) { cfg.Driver = "kafka"; cfg.SecondWorkerAfter = cfg.Duration },
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

func TestValidateFinalStats(t *testing.T) {
	if err := validateFinalStats(transport.TopologyStats{}); err != nil {
		t.Fatalf("settled stats: %v", err)
	}
	for name, stats := range map[string]transport.TopologyStats{
		"pending":   {Pending: 1},
		"delayed":   {Delayed: 1},
		"in flight": {InFlight: 1},
		"dlq":       {DLQ: 1},
		"lag":       {Lag: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateFinalStats(stats); err == nil {
				t.Fatalf("unsettled stats accepted: %+v", stats)
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

func TestKafkaPartitionsMatchRequestedConcurrency(t *testing.T) {
	cfg := config{Concurrency: 100}
	if got := kafkaPartitions(cfg); got != 0 {
		t.Fatalf("non-Kafka partitions = %d, want 0", got)
	}
	cfg.Driver = "kafka"
	if got := kafkaPartitions(cfg); got != 100 {
		t.Fatalf("Kafka partitions = %d, want 100", got)
	}
}

func TestKafkaDelayBatchUsesValidatedMaximum(t *testing.T) {
	cfg := config{}
	if got := kafkaDelayBatchSize(cfg); got != 0 {
		t.Fatalf("non-Kafka delay batch = %d, want 0", got)
	}
	cfg.Driver = "kafka"
	if got := kafkaDelayBatchSize(cfg); got != 1000 {
		t.Fatalf("Kafka delay batch = %d, want 1000", got)
	}
}

func TestNATSDelayBatchUsesValidatedMaximum(t *testing.T) {
	cfg := config{}
	if got := natsDelayBatchSize(cfg); got != 0 {
		t.Fatalf("non-NATS delay batch = %d, want 0", got)
	}
	cfg.Driver = "nats"
	if got := natsDelayBatchSize(cfg); got != 1000 {
		t.Fatalf("NATS delay batch = %d, want 1000", got)
	}
}

func TestRedisDelayBatchUsesValidatedMaximum(t *testing.T) {
	cfg := config{}
	if got := redisDelayBatchSize(cfg); got != 0 {
		t.Fatalf("non-Redis delay batch = %d, want 0", got)
	}
	cfg.Driver = "redis"
	if got := redisDelayBatchSize(cfg); got != 1000 {
		t.Fatalf("Redis delay batch = %d, want 1000", got)
	}
}

func TestStartBusAllowsKafkaTopicProvisioning(t *testing.T) {
	for _, test := range []struct {
		driver string
		want   time.Duration
	}{{driver: "redis", want: 20 * time.Second}, {driver: "kafka", want: 2 * time.Minute}} {
		t.Run(test.driver, func(t *testing.T) {
			bus := &startDeadlineBus{}
			if err := startBus(context.Background(), test.driver, bus); err != nil {
				t.Fatal(err)
			}
			if bus.remaining < test.want-time.Second || bus.remaining > test.want {
				t.Fatalf("startup deadline remaining = %v, want approximately %v", bus.remaining, test.want)
			}
		})
	}
}

type startDeadlineBus struct {
	eventbus.Bus
	remaining time.Duration
}

func (b *startDeadlineBus) Start(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("startup context has no deadline")
	}
	b.remaining = time.Until(deadline)
	return nil
}

func (*startDeadlineBus) Ping(context.Context) error { return nil }

func TestRetryEnqueueBoundsEachAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	var attempts int
	err := retryEnqueue(ctx, func(attemptCtx context.Context) error {
		attempts++
		if _, ok := attemptCtx.Deadline(); !ok {
			t.Fatal("enqueue attempt has no deadline")
		}
		return errors.New("temporary publish failure")
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retryEnqueue error = %v, want deadline exceeded", err)
	}
	if attempts < 2 {
		t.Fatalf("enqueue attempts = %d, want at least 2", attempts)
	}
}

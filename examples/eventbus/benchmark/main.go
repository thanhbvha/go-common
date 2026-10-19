package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	runtimemetrics "runtime/metrics"
	"strings"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/examples/eventbus/internal/demo"
)

type config struct {
	demo.Config
	PayloadKB         int
	Concurrency       int
	Workload          string
	Messages          int
	Duration          time.Duration
	Rate              int
	Timeout           time.Duration
	Preload           bool
	SecondWorkerAfter time.Duration
	Delay             time.Duration
	SampleEvery       time.Duration
	SamplesJSONL      string
}

type payload struct {
	Sequence       int    `json:"sequence"`
	EnqueuedUnixNS int64  `json:"enqueued_unix_ns"`
	Data           string `json:"data"`
	Retry          bool   `json:"retry,omitempty"`
}

type result struct {
	Driver               string  `json:"driver"`
	Namespace            string  `json:"namespace"`
	PayloadKB            int     `json:"payload_kb"`
	Concurrency          int     `json:"concurrency"`
	Workload             string  `json:"workload"`
	Messages             int     `json:"messages"`
	Mode                 string  `json:"mode"`
	RequestedMessages    int     `json:"requested_messages,omitempty"`
	DurationSeconds      float64 `json:"duration_seconds,omitempty"`
	TargetRatePerSecond  int     `json:"target_rate_per_second,omitempty"`
	Preloaded            bool    `json:"preloaded_backlog"`
	SecondWorker         bool    `json:"second_worker"`
	ElapsedSeconds       float64 `json:"elapsed_seconds"`
	ThroughputPerSecond  float64 `json:"throughput_per_second"`
	P50Milliseconds      float64 `json:"p50_milliseconds"`
	P95Milliseconds      float64 `json:"p95_milliseconds"`
	P99Milliseconds      float64 `json:"p99_milliseconds"`
	AllocBytesPerMessage float64 `json:"alloc_bytes_per_message"`
	AllocsPerMessage     float64 `json:"allocs_per_message"`
	PeakHeapBytes        uint64  `json:"peak_heap_bytes"`
	CPUSeconds           float64 `json:"cpu_seconds"`
	GCCycles             uint32  `json:"gc_cycles"`
	Duplicates           int64   `json:"duplicates"`
	PeakGoroutines       int     `json:"peak_goroutines"`
}

type periodicSample struct {
	Type                   string  `json:"type"`
	Timestamp              string  `json:"timestamp"`
	ElapsedSeconds         float64 `json:"elapsed_seconds"`
	Completed              int64   `json:"completed"`
	ThroughputPerSecond    float64 `json:"throughput_per_second"`
	HeapAllocBytes         uint64  `json:"heap_alloc_bytes"`
	Goroutines             int     `json:"goroutines"`
	TopologyStatsAvailable bool    `json:"topology_stats_available"`
	Pending                int64   `json:"pending,omitempty"`
	Delayed                int64   `json:"delayed,omitempty"`
	InFlight               int64   `json:"in_flight,omitempty"`
	DLQ                    int64   `json:"dlq,omitempty"`
	Lag                    int64   `json:"lag,omitempty"`
}

const (
	latencyHistogramBuckets = 512
	latencyHistogramMin     = time.Microsecond
	latencyHistogramMax     = 24 * time.Hour
	maxTrackedMessages      = 100_000_000
)

type latencyHistogram struct {
	buckets [latencyHistogramBuckets]atomic.Uint64
	count   atomic.Uint64
}

type completionCounter struct {
	count  atomic.Int64
	notify chan struct{}
}

type completionTracker struct {
	counter    *completionCounter
	seen       []atomic.Uint64
	duplicates atomic.Int64
}

func newCompletionCounter() *completionCounter {
	return &completionCounter{notify: make(chan struct{}, 1)}
}

func (c *completionCounter) Add() {
	c.count.Add(1)
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func (c *completionCounter) Wait(ctx context.Context, target int64) error {
	for c.count.Load() < target {
		select {
		case <-c.notify:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func newCompletionTracker(messages int) *completionTracker {
	return &completionTracker{
		counter: newCompletionCounter(),
		seen:    make([]atomic.Uint64, (messages+63)/64),
	}
}

func (t *completionTracker) Mark(sequence int) bool {
	word := &t.seen[sequence/64]
	mask := uint64(1) << uint(sequence%64)
	for {
		current := word.Load()
		if current&mask != 0 {
			t.duplicates.Add(1)
			return false
		}
		if word.CompareAndSwap(current, current|mask) {
			return true
		}
	}
}

func (h *latencyHistogram) Observe(value time.Duration) {
	h.buckets[latencyBucket(value)].Add(1)
	h.count.Add(1)
}

func (h *latencyHistogram) Percentile(quantile float64) float64 {
	count := h.count.Load()
	if count == 0 {
		return 0
	}
	target := uint64(math.Ceil(float64(count) * quantile))
	if target == 0 {
		target = 1
	}
	var seen uint64
	for index := range h.buckets {
		seen += h.buckets[index].Load()
		if seen >= target {
			return float64(latencyBucketUpper(index)) / float64(time.Millisecond)
		}
	}
	return float64(latencyHistogramMax) / float64(time.Millisecond)
}

func latencyBucket(value time.Duration) int {
	if value <= latencyHistogramMin {
		return 0
	}
	if value >= latencyHistogramMax {
		return latencyHistogramBuckets - 1
	}
	ratio := math.Log(float64(value)/float64(latencyHistogramMin)) / math.Log(float64(latencyHistogramMax)/float64(latencyHistogramMin))
	return 1 + int(ratio*float64(latencyHistogramBuckets-2))
}

func latencyBucketUpper(index int) time.Duration {
	if index <= 0 {
		return latencyHistogramMin
	}
	if index >= latencyHistogramBuckets-1 {
		return latencyHistogramMax
	}
	ratio := float64(index) / float64(latencyHistogramBuckets-2)
	return time.Duration(float64(latencyHistogramMin) * math.Pow(float64(latencyHistogramMax)/float64(latencyHistogramMin), ratio))
}

type quietMetrics struct{}

func (quietMetrics) IncEnqueue(string, string)       {}
func (quietMetrics) IncDequeue(string)               {}
func (quietMetrics) IncComplete(string, string)      {}
func (quietMetrics) ObserveDuration(string, float64) {}

type quietTracer struct{}

func (quietTracer) Start(ctx context.Context, _ string, _ eventbus.Headers) (context.Context, func()) {
	return ctx, func() {}
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "eventbus benchmark:", err)
		os.Exit(1)
	}
}

func run(args []string) (returnErr error) {
	cfg := config{Config: demo.DefaultConfig(), PayloadKB: 1, Concurrency: 10, Workload: "immediate", Messages: 1000, Timeout: 10 * time.Minute, Delay: time.Second, SampleEvery: 5 * time.Second}
	fs := flag.NewFlagSet("eventbus-benchmark", flag.ContinueOnError)
	demo.BindFlags(fs, &cfg.Config)
	fs.IntVar(&cfg.PayloadKB, "payload-kb", cfg.PayloadKB, "payload size: 1, 10, or 100 KiB")
	fs.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "workers: 1, 10, or 100")
	fs.StringVar(&cfg.Workload, "workload", cfg.Workload, "immediate, delayed, retry10, or retry50")
	fs.IntVar(&cfg.Messages, "messages", cfg.Messages, "unique messages to complete")
	fs.DurationVar(&cfg.Duration, "duration", 0, "rate-paced producer duration; zero selects finite message mode")
	fs.IntVar(&cfg.Rate, "rate", 0, "target messages per second for duration mode")
	fs.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "whole benchmark deadline")
	fs.BoolVar(&cfg.Preload, "preload", false, "pause intake and preload all messages before measuring consumption")
	fs.DurationVar(&cfg.SecondWorkerAfter, "second-worker-after", 0, "start a second Kafka consumer after this interval")
	fs.DurationVar(&cfg.Delay, "delay", cfg.Delay, "delay applied to delayed workload messages")
	fs.DurationVar(&cfg.SampleEvery, "sample-every", cfg.SampleEvery, "resource/topology sample interval")
	fs.StringVar(&cfg.SamplesJSONL, "samples-jsonl", "", "write periodic resource/topology samples to a new JSONL file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := validate(cfg); err != nil {
		return err
	}
	if cfg.Namespace == "example_eventbus" {
		cfg.Namespace = fmt.Sprintf("eventbus_bench_%s_%d", cfg.Driver, time.Now().UnixNano())
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	logger := demo.NewLoggerTo("benchmark", io.Discard)
	rt, err := demo.OpenRuntime(ctx, cfg.Config, logger)
	if err != nil {
		return err
	}
	var bus, secondBus eventbus.Bus
	var secondRuntime *demo.Runtime
	type workerStart struct {
		bus     eventbus.Bus
		runtime *demo.Runtime
		err     error
	}
	var secondWorkerResult chan workerStart
	var secondWorkerCancel context.CancelFunc
	secondWorkerPending := false
	defer func() {
		if secondWorkerPending {
			secondWorkerCancel()
			worker := <-secondWorkerResult
			secondBus, secondRuntime = worker.bus, worker.runtime
			if worker.err != nil && !errors.Is(worker.err, context.Canceled) {
				returnErr = errors.Join(returnErr, fmt.Errorf("start second worker: %w", worker.err))
			}
		}
		if secondRuntime != nil {
			returnErr = errors.Join(returnErr, demo.CloseAll(secondBus, secondRuntime))
		}
		returnErr = errors.Join(returnErr, demo.CloseAll(bus, rt))
	}()

	topologyOptions := append(demo.TopologyOptions(cfg.Driver),
		eventbus.WithConcurrency(cfg.Concurrency),
		eventbus.WithPrefetch(cfg.Concurrency*2),
		eventbus.WithMaxAttempts(2),
		eventbus.WithMaxPayloadBytes(128*1024),
	)
	var latencies latencyHistogram
	sequenceLimit, err := plannedMessages(cfg)
	if err != nil {
		return err
	}
	completed := newCompletionTracker(sequenceLimit)
	windowSize := min(sequenceLimit, 900)
	window := make(chan struct{}, windowSize)
	handler := eventbus.Handle(func(_ context.Context, job eventbus.Job, item payload) error {
		if item.Sequence < 0 || item.Sequence >= sequenceLimit || item.EnqueuedUnixNS <= 0 {
			return eventbus.MarkPermanent(fmt.Errorf("sequence outside benchmark range"))
		}
		if item.Retry && job.Attempt() == 1 {
			return errors.New("synthetic benchmark retry")
		}
		if !completed.Mark(item.Sequence) {
			return nil
		}
		latencies.Observe(time.Since(time.Unix(0, item.EnqueuedUnixNS)))
		if !cfg.Preload {
			<-window
		}
		completed.counter.Add()
		return nil
	})
	const topology = "phase6_benchmark"
	openBus := func(runtime *demo.Runtime) (eventbus.Bus, error) {
		busCfg := eventbus.DefaultConfig()
		busCfg.Namespace = cfg.Namespace
		busCfg.Concurrency = cfg.Concurrency
		busCfg.Prefetch = cfg.Concurrency * 2
		busCfg.MaxAttempts = 2
		busCfg.MaxPayloadBytes = 128 * 1024
		busCfg.ProcessTimeout = time.Minute
		busCfg.RetryPolicy = eventbus.ConstantBackoff{}
		busCfg.Logger = logger
		busCfg.Metrics = quietMetrics{}
		busCfg.Tracer = quietTracer{}
		worker, openErr := eventbus.Open(busCfg, eventbus.WithTransport(runtime.Transport))
		if openErr != nil {
			return nil, openErr
		}
		if openErr = worker.DeclareTask(topology, topologyOptions...); openErr != nil {
			return nil, errors.Join(openErr, worker.Close(context.Background()))
		}
		if openErr = worker.HandleTask(topology, handler); openErr != nil {
			return nil, errors.Join(openErr, worker.Close(context.Background()))
		}
		return worker, nil
	}
	bus, err = openBus(rt)
	if err != nil {
		return err
	}
	if err := demo.Start(ctx, bus); err != nil {
		return err
	}
	if cfg.Preload {
		if err := bus.Pause(ctx, topology); err != nil {
			return fmt.Errorf("pause before preload: %w", err)
		}
	}

	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	cpuBefore := cpuSeconds()
	var sampleFile *os.File
	if cfg.SamplesJSONL != "" {
		sampleFile, err = os.OpenFile(cfg.SamplesJSONL, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("create samples JSONL: %w", err)
		}
		defer func() { returnErr = errors.Join(returnErr, sampleFile.Close()) }()
	}
	resourceDone := make(chan resourcePeaks, 1)
	stopResources := make(chan struct{})
	benchmarkStart := time.Now()
	go sampleResources(ctx, stopResources, bus, topology, benchmarkStart, cfg.SampleEvery, before.HeapAlloc, completed.counter, sampleFile, resourceDone)
	resourcesStopped := false
	defer func() {
		if !resourcesStopped {
			close(stopResources)
			<-resourceDone
		}
	}()

	if cfg.SecondWorkerAfter > 0 {
		secondWorkerResult = make(chan workerStart, 1)
		workerCtx, cancelWorker := context.WithCancel(ctx)
		secondWorkerCancel = cancelWorker
		secondWorkerPending = true
		go func() {
			timer := time.NewTimer(cfg.SecondWorkerAfter)
			defer timer.Stop()
			select {
			case <-workerCtx.Done():
				secondWorkerResult <- workerStart{err: workerCtx.Err()}
				return
			case <-timer.C:
			}
			workerRuntime, workerErr := demo.OpenRuntime(workerCtx, cfg.Config, logger)
			var workerBus eventbus.Bus
			if workerErr == nil {
				workerBus, workerErr = openBus(workerRuntime)
			}
			if workerErr == nil {
				workerErr = demo.Start(workerCtx, workerBus)
			}
			secondWorkerResult <- workerStart{bus: workerBus, runtime: workerRuntime, err: workerErr}
		}()
	}

	body := strings.Repeat("x", cfg.PayloadKB*1024)
	produced := 0
	producerStarted := time.Now()
	producerDeadline := producerStarted.Add(cfg.Duration)
	nextEnqueue := producerStarted
	for produced < sequenceLimit {
		if cfg.Duration > 0 {
			if !nextEnqueue.Before(producerDeadline) {
				break
			}
			if err := waitUntil(ctx, nextEnqueue); err != nil {
				return err
			}
		}
		if !cfg.Preload {
			select {
			case window <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		item := payload{Sequence: produced, EnqueuedUnixNS: time.Now().UnixNano(), Data: body, Retry: shouldRetry(cfg.Workload, produced)}
		var opts []eventbus.JobOption
		if cfg.Workload == "delayed" {
			opts = append(opts, eventbus.WithDelay(cfg.Delay))
		}
		if err := enqueueWithRetry(ctx, bus, topology, item, opts...); err != nil {
			if !cfg.Preload {
				<-window
			}
			return fmt.Errorf("enqueue message %d: %w", produced, err)
		}
		produced++
		if cfg.Duration > 0 {
			nextEnqueue = nextEnqueue.Add(time.Second / time.Duration(cfg.Rate))
			if now := time.Now(); nextEnqueue.Before(now) {
				nextEnqueue = now
			}
		}
	}
	if cfg.SecondWorkerAfter > 0 {
		worker := <-secondWorkerResult
		secondWorkerPending = false
		secondWorkerCancel()
		secondBus, secondRuntime = worker.bus, worker.runtime
		if worker.err != nil {
			return fmt.Errorf("start second worker: %w", worker.err)
		}
	}
	if cfg.Preload {
		if err := bus.DrainTopology(ctx, topology); err != nil {
			return fmt.Errorf("drain preloaded topology: %w", err)
		}
		benchmarkStart = time.Now()
		if err := bus.Resume(ctx, topology); err != nil {
			return fmt.Errorf("resume preloaded topology: %w", err)
		}
	}
	if err := completed.counter.Wait(ctx, int64(produced)); err != nil {
		return err
	}
	elapsed := time.Since(benchmarkStart)
	if err := bus.Drain(ctx); err != nil {
		return err
	}
	close(stopResources)
	peaks := <-resourceDone
	resourcesStopped = true
	cpuUsed := cpuSeconds() - cpuBefore
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	report := result{
		Driver: cfg.Driver, Namespace: cfg.Namespace, PayloadKB: cfg.PayloadKB,
		Concurrency: cfg.Concurrency, Workload: cfg.Workload, Messages: produced, Preloaded: cfg.Preload,
		Mode: benchmarkMode(cfg), RequestedMessages: requestedMessages(cfg), DurationSeconds: cfg.Duration.Seconds(), TargetRatePerSecond: cfg.Rate,
		SecondWorker:   cfg.SecondWorkerAfter > 0,
		ElapsedSeconds: elapsed.Seconds(), ThroughputPerSecond: float64(produced) / elapsed.Seconds(),
		P50Milliseconds: latencies.Percentile(0.50), P95Milliseconds: latencies.Percentile(0.95), P99Milliseconds: latencies.Percentile(0.99),
		AllocBytesPerMessage: float64(after.TotalAlloc-before.TotalAlloc) / float64(produced),
		AllocsPerMessage:     float64(after.Mallocs-before.Mallocs) / float64(produced),
		PeakHeapBytes:        peaks.heap, CPUSeconds: max(0, cpuUsed), GCCycles: after.NumGC - before.NumGC,
		Duplicates: completed.duplicates.Load(), PeakGoroutines: peaks.goroutines,
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func validate(cfg config) error {
	if err := cfg.Config.Validate(); err != nil {
		return err
	}
	if cfg.PayloadKB != 1 && cfg.PayloadKB != 10 && cfg.PayloadKB != 100 {
		return fmt.Errorf("payload-kb must be 1, 10, or 100")
	}
	if cfg.Concurrency != 1 && cfg.Concurrency != 10 && cfg.Concurrency != 100 {
		return fmt.Errorf("concurrency must be 1, 10, or 100")
	}
	switch cfg.Workload {
	case "immediate", "delayed", "retry10", "retry50":
	default:
		return fmt.Errorf("unsupported workload %q", cfg.Workload)
	}
	if cfg.Messages <= 0 {
		return fmt.Errorf("messages must be positive")
	}
	if cfg.Duration < 0 {
		return fmt.Errorf("duration must not be negative")
	}
	if cfg.Duration == 0 && cfg.Rate != 0 {
		return fmt.Errorf("rate requires duration mode")
	}
	if cfg.Duration > 0 && cfg.Rate <= 0 {
		return fmt.Errorf("rate must be positive in duration mode")
	}
	if cfg.Duration > 0 && cfg.Preload {
		return fmt.Errorf("preload is not supported in duration mode")
	}
	if cfg.Rate > 1_000_000 {
		return fmt.Errorf("rate must not exceed 1000000 messages per second")
	}
	if cfg.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if cfg.Duration > 0 && cfg.Timeout <= cfg.Duration {
		return fmt.Errorf("timeout must exceed duration to allow backlog drain")
	}
	if cfg.SecondWorkerAfter < 0 {
		return fmt.Errorf("second-worker-after must not be negative")
	}
	if cfg.SecondWorkerAfter > 0 && cfg.Driver != "kafka" {
		return fmt.Errorf("second-worker-after is supported only for kafka")
	}
	if cfg.SecondWorkerAfter > 0 && cfg.Preload {
		return fmt.Errorf("second-worker-after cannot be combined with preload")
	}
	if cfg.SecondWorkerAfter >= cfg.Timeout {
		return fmt.Errorf("second-worker-after must be less than timeout")
	}
	if cfg.Delay <= 0 {
		return fmt.Errorf("delay must be positive")
	}
	if cfg.SampleEvery <= 0 {
		return fmt.Errorf("sample-every must be positive")
	}
	if _, err := plannedMessages(cfg); err != nil {
		return err
	}
	return nil
}

func benchmarkMode(cfg config) string {
	if cfg.Duration > 0 {
		return "duration"
	}
	return "messages"
}

func requestedMessages(cfg config) int {
	if cfg.Duration > 0 {
		return 0
	}
	return cfg.Messages
}

func plannedMessages(cfg config) (int, error) {
	planned := int64(cfg.Messages)
	if cfg.Duration > 0 {
		seconds := float64(cfg.Duration) / float64(time.Second)
		planned = int64(math.Ceil(seconds * float64(cfg.Rate)))
		planned = max(planned, 1)
	}
	if planned > maxTrackedMessages {
		return 0, fmt.Errorf("verification state would track %d messages; maximum is %d", planned, maxTrackedMessages)
	}
	return int(planned), nil
}

func waitUntil(ctx context.Context, deadline time.Time) error {
	delay := time.Until(deadline)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func shouldRetry(workload string, sequence int) bool {
	switch workload {
	case "retry10":
		return sequence%10 == 0
	case "retry50":
		return sequence%2 == 0
	default:
		return false
	}
}

func enqueueWithRetry(ctx context.Context, bus eventbus.Bus, topology string, item payload, opts ...eventbus.JobOption) error {
	backoff := 25 * time.Millisecond
	for {
		err := bus.Enqueue(ctx, topology, item, opts...)
		if err == nil {
			return nil
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		backoff = min(500*time.Millisecond, backoff*2)
	}
}

func cpuSeconds() float64 {
	samples := []runtimemetrics.Sample{{Name: "/cpu/classes/total:cpu-seconds"}, {Name: "/cpu/classes/idle:cpu-seconds"}}
	runtimemetrics.Read(samples)
	return samples[0].Value.Float64() - samples[1].Value.Float64()
}

type resourcePeaks struct {
	heap       uint64
	goroutines int
}

func sampleResources(ctx context.Context, stop <-chan struct{}, bus eventbus.Bus, topology string, started time.Time, interval time.Duration, initialHeap uint64, completed *completionCounter, output io.Writer, result chan<- resourcePeaks) {
	peaks := resourcePeaks{heap: initialHeap, goroutines: runtime.NumGoroutine()}
	peakTicker := time.NewTicker(min(100*time.Millisecond, interval))
	defer peakTicker.Stop()
	sampleTicker := time.NewTicker(interval)
	defer sampleTicker.Stop()
	var encoder *json.Encoder
	if output != nil {
		encoder = json.NewEncoder(output)
	}
	sample := func(write bool) {
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		goroutines := runtime.NumGoroutine()
		peaks.heap = max(peaks.heap, stats.HeapAlloc)
		peaks.goroutines = max(peaks.goroutines, goroutines)
		if !write || encoder == nil {
			return
		}
		elapsed := time.Since(started)
		record := periodicSample{
			Type: "sample", Timestamp: time.Now().UTC().Format(time.RFC3339Nano), ElapsedSeconds: elapsed.Seconds(),
			Completed: completed.count.Load(), HeapAllocBytes: stats.HeapAlloc, Goroutines: goroutines,
		}
		if elapsed > 0 {
			record.ThroughputPerSecond = float64(record.Completed) / elapsed.Seconds()
		}
		statsCtx, cancel := context.WithTimeout(ctx, min(2*time.Second, interval))
		topologyStats, err := bus.Stats(statsCtx, topology)
		cancel()
		if err == nil {
			record.TopologyStatsAvailable = true
			record.Pending = topologyStats.Pending
			record.Delayed = topologyStats.Delayed
			record.InFlight = topologyStats.InFlight
			record.DLQ = topologyStats.DLQ
			record.Lag = topologyStats.Lag
		}
		_ = encoder.Encode(record)
	}
	for {
		select {
		case <-peakTicker.C:
			sample(false)
		case <-sampleTicker.C:
			sample(true)
		case <-stop:
			sample(true)
			result <- peaks
			return
		}
	}
}

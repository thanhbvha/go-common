package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	runtimemetrics "runtime/metrics"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/examples/eventbus/internal/demo"
)

type config struct {
	demo.Config
	PayloadKB   int
	Concurrency int
	Workload    string
	Messages    int
	Timeout     time.Duration
	Preload     bool
}

type payload struct {
	Sequence int    `json:"sequence"`
	Data     string `json:"data"`
	Retry    bool   `json:"retry,omitempty"`
}

type result struct {
	Driver               string  `json:"driver"`
	Namespace            string  `json:"namespace"`
	PayloadKB            int     `json:"payload_kb"`
	Concurrency          int     `json:"concurrency"`
	Workload             string  `json:"workload"`
	Messages             int     `json:"messages"`
	Preloaded            bool    `json:"preloaded_backlog"`
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
	cfg := config{Config: demo.DefaultConfig(), PayloadKB: 1, Concurrency: 10, Workload: "immediate", Messages: 1000, Timeout: 10 * time.Minute}
	fs := flag.NewFlagSet("eventbus-benchmark", flag.ContinueOnError)
	demo.BindFlags(fs, &cfg.Config)
	fs.IntVar(&cfg.PayloadKB, "payload-kb", cfg.PayloadKB, "payload size: 1, 10, or 100 KiB")
	fs.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "workers: 1, 10, or 100")
	fs.StringVar(&cfg.Workload, "workload", cfg.Workload, "immediate, delayed, retry10, or retry50")
	fs.IntVar(&cfg.Messages, "messages", cfg.Messages, "unique messages to complete")
	fs.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "whole benchmark deadline")
	fs.BoolVar(&cfg.Preload, "preload", false, "pause intake and preload all messages before measuring consumption")
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
	var bus eventbus.Bus
	defer func() { returnErr = errors.Join(returnErr, demo.CloseAll(bus, rt)) }()

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
	bus, err = eventbus.Open(busCfg, eventbus.WithTransport(rt.Transport))
	if err != nil {
		return err
	}

	topologyOptions := append(demo.TopologyOptions(cfg.Driver),
		eventbus.WithConcurrency(cfg.Concurrency),
		eventbus.WithPrefetch(cfg.Concurrency*2),
		eventbus.WithMaxAttempts(2),
		eventbus.WithMaxPayloadBytes(128*1024),
	)
	const topology = "phase6_benchmark"
	if err := bus.DeclareTask(topology, topologyOptions...); err != nil {
		return err
	}

	started := make([]time.Time, cfg.Messages)
	latencies := make([]atomic.Int64, cfg.Messages)
	completed := make(chan struct{}, cfg.Messages)
	windowSize := min(cfg.Messages, 900)
	if cfg.Preload {
		windowSize = cfg.Messages
	}
	window := make(chan struct{}, windowSize)
	var duplicates atomic.Int64
	if err := bus.HandleTask(topology, eventbus.Handle(func(_ context.Context, job eventbus.Job, item payload) error {
		if item.Sequence < 0 || item.Sequence >= len(latencies) {
			return eventbus.MarkPermanent(fmt.Errorf("sequence outside benchmark range"))
		}
		if item.Retry && job.Attempt() == 1 {
			return errors.New("synthetic benchmark retry")
		}
		elapsed := time.Since(started[item.Sequence]).Nanoseconds()
		if latencies[item.Sequence].CompareAndSwap(0, elapsed) {
			<-window
			completed <- struct{}{}
		} else {
			duplicates.Add(1)
		}
		return nil
	})); err != nil {
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
	peakDone := make(chan uint64, 1)
	stopPeak := make(chan struct{})
	go samplePeakHeap(stopPeak, before.HeapAlloc, peakDone)
	peakStopped := false
	defer func() {
		if !peakStopped {
			close(stopPeak)
			<-peakDone
		}
	}()

	body := strings.Repeat("x", cfg.PayloadKB*1024)
	delay := rt.Transport.Capabilities().DelayPrecision
	if delay < time.Millisecond {
		delay = time.Millisecond
	}
	benchmarkStart := time.Now()
	for i := 0; i < cfg.Messages; i++ {
		select {
		case window <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		item := payload{Sequence: i, Data: body, Retry: shouldRetry(cfg.Workload, i)}
		started[i] = time.Now()
		var opts []eventbus.JobOption
		if cfg.Workload == "delayed" {
			opts = append(opts, eventbus.WithDelay(delay))
		}
		if err := enqueueWithRetry(ctx, bus, topology, item, opts...); err != nil {
			<-window
			return fmt.Errorf("enqueue message %d: %w", i, err)
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
	for range cfg.Messages {
		select {
		case <-completed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	elapsed := time.Since(benchmarkStart)
	if err := bus.Drain(ctx); err != nil {
		return err
	}
	close(stopPeak)
	peak := <-peakDone
	peakStopped = true
	cpuUsed := cpuSeconds() - cpuBefore
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	values := make([]time.Duration, cfg.Messages)
	for i := range latencies {
		values[i] = time.Duration(latencies[i].Load())
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	report := result{
		Driver: cfg.Driver, Namespace: cfg.Namespace, PayloadKB: cfg.PayloadKB,
		Concurrency: cfg.Concurrency, Workload: cfg.Workload, Messages: cfg.Messages, Preloaded: cfg.Preload,
		ElapsedSeconds: elapsed.Seconds(), ThroughputPerSecond: float64(cfg.Messages) / elapsed.Seconds(),
		P50Milliseconds: percentile(values, 0.50), P95Milliseconds: percentile(values, 0.95), P99Milliseconds: percentile(values, 0.99),
		AllocBytesPerMessage: float64(after.TotalAlloc-before.TotalAlloc) / float64(cfg.Messages),
		AllocsPerMessage:     float64(after.Mallocs-before.Mallocs) / float64(cfg.Messages),
		PeakHeapBytes:        peak, CPUSeconds: max(0, cpuUsed), GCCycles: after.NumGC - before.NumGC,
		Duplicates: duplicates.Load(),
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
	if cfg.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	return nil
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

func percentile(sorted []time.Duration, quantile float64) float64 {
	index := int(float64(len(sorted)-1) * quantile)
	return float64(sorted[index]) / float64(time.Millisecond)
}

func cpuSeconds() float64 {
	samples := []runtimemetrics.Sample{{Name: "/cpu/classes/total:cpu-seconds"}, {Name: "/cpu/classes/idle:cpu-seconds"}}
	runtimemetrics.Read(samples)
	return samples[0].Value.Float64() - samples[1].Value.Float64()
}

func samplePeakHeap(stop <-chan struct{}, initial uint64, result chan<- uint64) {
	peak := initial
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			if stats.HeapAlloc > peak {
				peak = stats.HeapAlloc
			}
		case <-stop:
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			if stats.HeapAlloc > peak {
				peak = stats.HeapAlloc
			}
			result <- peak
			return
		}
	}
}

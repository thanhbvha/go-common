// Package perftest supports opt-in performance fixtures and separate load processes.
// It is internal tooling, not part of the library's public API.
package perftest

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Histogram has fixed memory and logarithmic buckets from 1 ns to 1 minute.
// Percentiles return bucket upper bounds (less than 1.3% relative bucket width).
type Histogram struct {
	buckets [2048]atomic.Uint64
	count   atomic.Uint64
}

func (h *Histogram) Observe(d time.Duration) {
	i := int(math.Log1p(float64(max(d, 0))) / math.Log1p(float64(time.Minute)) * 2047)
	h.buckets[min(i, 2047)].Add(1)
	h.count.Add(1)
}
func (h *Histogram) Percentile(q float64) float64 {
	target := uint64(math.Ceil(float64(h.count.Load()) * q))
	if target == 0 {
		return 0
	}
	var n uint64
	for i := range h.buckets {
		n += h.buckets[i].Load()
		if n >= target {
			return math.Expm1(float64(i+1)/2047*math.Log1p(float64(time.Minute))) / float64(time.Millisecond)
		}
	}
	return float64(time.Minute) / float64(time.Millisecond)
}

type Resources struct {
	HeapBefore       uint64 `json:"heap_before_bytes"`
	HeapAfter        uint64 `json:"heap_after_bytes"`
	PeakHeap         uint64 `json:"peak_heap_bytes"`
	AllocBytes       uint64 `json:"allocated_bytes"`
	Allocs           uint64 `json:"allocations"`
	GoroutinesBefore int    `json:"goroutines_before"`
	GoroutinesAfter  int    `json:"goroutines_after"`
	PeakGoroutines   int    `json:"peak_goroutines"`
	GCCycles         uint32 `json:"gc_cycles"`
}
type Result struct {
	Name             string    `json:"name"`
	Completed        uint64    `json:"completed"`
	Errors           uint64    `json:"errors"`
	Duplicates       uint64    `json:"duplicates"`
	Missing          uint64    `json:"missing"`
	Throughput       float64   `json:"throughput_per_second"`
	P50              float64   `json:"p50_ms"`
	P95              float64   `json:"p95_ms"`
	P99              float64   `json:"p99_ms"`
	Elapsed          float64   `json:"elapsed_seconds"`
	Drain            float64   `json:"drain_seconds"`
	Pending          int64     `json:"pending"`
	InFlight         int64     `json:"in_flight"`
	Delayed          int64     `json:"delayed"`
	DLQ              int64     `json:"dlq"`
	BacklogMeasured  bool      `json:"backlog_measured,omitempty"`
	BrokerDrain      float64   `json:"broker_drain_seconds,omitempty"`
	SentOperations   uint64    `json:"sent_operations,omitempty"`
	ExpectedReceipts uint64    `json:"expected_receipts,omitempty"`
	Received         uint64    `json:"received,omitempty"`
	Server           Resources `json:"server"`
	Client           Resources `json:"client"`
}

func WriteJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0600)
}
func Duration(key string) time.Duration {
	seconds := 0.0
	if _, err := fmt.Sscan(os.Getenv(key), &seconds); err != nil {
		panic("missing/invalid " + key)
	}
	return time.Duration(seconds * float64(time.Second))
}
func Require(t testing.TB) string {
	t.Helper()
	if os.Getenv("PERF_RUN") != "1" {
		t.Skip("run benchmarks/performance.py for isolated fixtures")
	}
	if os.Getenv("PERF_OUTPUT") == "" {
		t.Fatal("PERF_OUTPUT is required")
	}
	return os.Getenv("PERF_OUTPUT")
}
func Await(ctx context.Context, path string) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Load runs one closed-loop operation per worker with bounded in-flight work.
// Each operation must honor its context; drain is bounded by the 15-second budget.
// It is called in a separate client process, never the profiled server process.
func Load(name string, operations []func(context.Context) error) (Result, error) {
	return LoadPaced(name, operations, 0)
}

// LoadPaced limits each worker to rate operations/second; zero selects saturation.
func LoadPaced(name string, operations []func(context.Context) error, rate float64) (Result, error) {
	phase := os.Getenv("PERF_PHASE_DIR")
	if phase == "" || len(operations) == 0 {
		return Result{}, fmt.Errorf("phase directory and operations required")
	}
	duration, warmup := Duration("PERF_DURATION"), Duration("PERF_WARMUP")
	var histogram Histogram
	var completed, failures atomic.Uint64
	var firstFailure atomic.Pointer[string]
	window := func(d time.Duration, measure bool) {
		deadline := time.Now().Add(d)
		var wg sync.WaitGroup
		for _, operation := range operations {
			wg.Go(func() {
				next := time.Now()
				for time.Now().Before(deadline) && next.Before(deadline) {
					if wait := time.Until(next); wait > 0 {
						time.Sleep(wait)
					}
					if rate > 0 {
						next = next.Add(time.Duration(float64(time.Second) / rate))
					}
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					started := time.Now()
					err := operation(ctx)
					cancel()
					if err != nil {
						failures.Add(1)
						message := err.Error()
						firstFailure.CompareAndSwap(nil, &message)
						return
					}
					if measure {
						histogram.Observe(time.Since(started))
						completed.Add(1)
					}
				}
			})
		}
		wg.Wait()
		if wait := time.Until(deadline); wait > 0 {
			time.Sleep(wait)
		}
	}
	window(warmup, false)
	if failures.Load() != 0 {
		return Result{}, fmt.Errorf("warmup failed: %s", *firstFailure.Load())
	}
	if err := os.WriteFile(filepath.Join(phase, "ready"), nil, 0600); err != nil {
		return Result{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := Await(ctx, filepath.Join(phase, "start")); err != nil {
		return Result{}, err
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	goroutines := runtime.NumGoroutine()
	var peakHeap atomic.Uint64
	var peakGoroutines atomic.Int64
	peakHeap.Store(before.HeapAlloc)
	peakGoroutines.Store(int64(goroutines))
	stopSampler := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampler:
				return
			case <-ticker.C:
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				peakHeap.Store(max(peakHeap.Load(), mem.HeapAlloc))
				peakGoroutines.Store(max(peakGoroutines.Load(), int64(runtime.NumGoroutine())))
			}
		}
	}()
	start := time.Now()
	window(duration, true)
	elapsed := time.Since(start)
	close(stopSampler)
	<-sampled
	runtime.ReadMemStats(&after)
	result := Result{Name: name, Completed: completed.Load(), Errors: failures.Load(), Throughput: float64(completed.Load()) / elapsed.Seconds(), P50: histogram.Percentile(.5), P95: histogram.Percentile(.95), P99: histogram.Percentile(.99), Elapsed: elapsed.Seconds(), Drain: max(0, elapsed.Seconds()-duration.Seconds()), Client: Resources{HeapBefore: before.HeapAlloc, HeapAfter: after.HeapAlloc, PeakHeap: max(peakHeap.Load(), after.HeapAlloc), PeakGoroutines: int(peakGoroutines.Load()), AllocBytes: after.TotalAlloc - before.TotalAlloc, Allocs: after.Mallocs - before.Mallocs, GoroutinesBefore: goroutines, GoroutinesAfter: runtime.NumGoroutine(), GCCycles: after.NumGC - before.NumGC}}
	if result.Completed == 0 || result.Errors != 0 || result.Drain > 15 {
		return result, fmt.Errorf("incomplete workload: completed=%d errors=%d drain=%f", result.Completed, result.Errors, result.Drain)
	}
	return result, nil
}

// Child starts a client after server setup, waits for warmup, and profiles only
// this server process. Its phase directory is unique to this scenario/run.
func Child(t *testing.T, name string, cmd *exec.Cmd) Result {
	t.Helper()
	return ChildWithDrain(t, name, cmd, nil)
}

// Backlog is an observed broker snapshot, never inferred from client receipt.
type Backlog struct{ Pending, InFlight, Delayed, DLQ int64 }

// ChildWithDrain keeps WebSocket clients connected while the caller verifies
// broker settlement. The client announces load-complete and waits for the
// broker-drained barrier, continuing to detect unsolicited replies afterwards.
func ChildWithDrain(t *testing.T, name string, cmd *exec.Cmd, drain func(context.Context) (Backlog, error)) Result {
	t.Helper()
	output := Require(t)
	dir := filepath.Join(output, name)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(os.Environ(), "PERF_PHASE_DIR="+dir)
	stdout, err := os.Create(filepath.Join(dir, "client.stdout"))
	if err != nil {
		t.Fatal(err)
	}
	defer stdout.Close()
	stderr, err := os.Create(filepath.Join(dir, "client.stderr"))
	if err != nil {
		t.Fatal(err)
	}
	defer stderr.Close()
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		default:
		}
	}()
	ready := filepath.Join(dir, "ready")
	deadline := time.NewTimer(Duration("PERF_WARMUP") + 30*time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
waitReady:
	for {
		select {
		case err := <-done:
			t.Fatalf("client exited before warmup completed: %v; see %s", err, dir)
		case <-deadline.C:
			t.Fatal("client warmup timeout")
		case <-tick.C:
			if _, err := os.Stat(ready); err == nil {
				break waitReady
			}
		}
	}
	tick.Reset(100 * time.Millisecond)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	resources := Resources{HeapBefore: before.HeapAlloc, GoroutinesBefore: runtime.NumGoroutine(), PeakHeap: before.HeapAlloc, PeakGoroutines: runtime.NumGoroutine()}
	profile := os.Getenv("PERF_PROFILE")
	var cpu *os.File
	switch profile {
	case "cpu":
		cpu, err = os.Create(filepath.Join(dir, "cpu.pprof"))
		if err != nil {
			t.Fatal(err)
		}
		if err = pprof.StartCPUProfile(cpu); err != nil {
			cpu.Close()
			t.Fatal(err)
		}
		defer func() {
			if cpu != nil {
				pprof.StopCPUProfile()
				cpu.Close()
			}
		}()
	case "memory":
		old := runtime.MemProfileRate
		runtime.MemProfileRate = 1
		defer func() { runtime.MemProfileRate = old }()
	case "mutex":
		old := runtime.SetMutexProfileFraction(1)
		defer runtime.SetMutexProfileFraction(old)
	case "block":
		runtime.SetBlockProfileRate(1)
		defer runtime.SetBlockProfileRate(0)
	case "":
	default:
		t.Fatal("invalid profile kind")
	}
	if err := os.WriteFile(filepath.Join(dir, "start"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	sampledLive := false
	var backlog Backlog
	var brokerDrain time.Duration
	drained := false
	started := time.Now()
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("load client: %v; see %s", err, dir)
			}
			goto finished
		case <-tick.C:
			if drain != nil && !drained {
				if _, err := os.Stat(filepath.Join(dir, "load-complete")); err == nil {
					drainStarted := time.Now()
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					backlog, err = drain(ctx)
					cancel()
					if err != nil {
						t.Fatalf("broker drain: %v", err)
					}
					brokerDrain = time.Since(drainStarted)
					if backlog != (Backlog{}) {
						t.Fatalf("broker did not drain: %+v", backlog)
					}
					if err := os.WriteFile(filepath.Join(dir, "broker-drained"), nil, 0600); err != nil {
						t.Fatal(err)
					}
					drained = true
				}
			}
			runtime.ReadMemStats(&after)
			resources.PeakHeap = max(resources.PeakHeap, after.HeapAlloc)
			resources.PeakGoroutines = max(resources.PeakGoroutines, runtime.NumGoroutine())
			if time.Since(started) > Duration("PERF_DURATION")/2 && !sampledLive && profile != "" {
				dump(t, dir, "heap-live", "heap")
				sampledLive = true
			}
			if time.Since(started) > Duration("PERF_DURATION")+20*time.Second {
				t.Fatal("load/drain timeout")
			}
		}
	}
finished:
	if drain != nil && !drained {
		t.Fatal("client exited without broker drain barrier")
	}
	if cpu != nil {
		pprof.StopCPUProfile()
		cpu.Close()
		cpu = nil
	}
	runtime.ReadMemStats(&after)
	resources.AllocBytes = after.TotalAlloc - before.TotalAlloc
	resources.Allocs = after.Mallocs - before.Mallocs
	resources.GCCycles = after.NumGC - before.NumGC
	runtime.GC()
	runtime.ReadMemStats(&after)
	resources.HeapAfter = after.HeapAlloc
	resources.GoroutinesAfter = runtime.NumGoroutine()
	if profile != "" {
		dump(t, dir, "heap-drained", "heap")
		kind := map[string]string{"cpu": "allocs", "memory": "allocs", "mutex": "mutex", "block": "block"}[profile]
		dump(t, dir, kind, kind)
	}
	var result Result
	data, err := os.ReadFile(filepath.Join(dir, "client.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	result.Server = resources
	if drain != nil {
		result.Pending, result.InFlight, result.Delayed, result.DLQ = backlog.Pending, backlog.InFlight, backlog.Delayed, backlog.DLQ
		result.BacklogMeasured, result.BrokerDrain = true, brokerDrain.Seconds()
	}
	if result.Name != name || result.Completed == 0 || result.Errors != 0 || result.Missing != 0 || result.Duplicates != 0 {
		t.Fatal("invalid/incomplete client result")
	}
	return result
}
func dump(t *testing.T, dir, name, kind string) {
	t.Helper()
	f, err := os.Create(filepath.Join(dir, name+".pprof"))
	if err != nil {
		t.Fatal(err)
	}
	if err = pprof.Lookup(kind).WriteTo(f, 0); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
}

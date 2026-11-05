package perftest

import (
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestHistogramConcurrentAndBounded(t *testing.T) {
	var h Histogram
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				h.Observe(time.Millisecond)
			}
		})
	}
	wg.Wait()
	if h.count.Load() != 8000 {
		t.Fatal("samples lost")
	}
	for _, q := range []float64{.5, .95, .99} {
		if v := h.Percentile(q); v < 1 || v > 1.013 {
			t.Fatalf("percentile upper bound %f", v)
		}
	}
	var empty Histogram
	if empty.Percentile(.99) != 0 {
		t.Fatal("empty histogram")
	}
	h.Observe(-time.Second)
	h.Observe(time.Hour)
	if v := h.Percentile(1); math.IsNaN(v) || math.IsInf(v, 0) || v < 60000 || v > 61000 {
		t.Fatalf("overflow bucket %f", v)
	}
}
func TestLoadPhasesAndErrors(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PERF_PHASE_DIR", dir)
	t.Setenv("PERF_DURATION", "0.01")
	t.Setenv("PERF_WARMUP", "0.002")
	if err := os.WriteFile(filepath.Join(dir, "start"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	result, err := Load("test", []func(context.Context) error{func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
			return nil
		}
	}})
	if err != nil || result.Completed == 0 || result.Errors != 0 || result.P99 < 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ready")); err != nil {
		t.Fatal("warmup readiness missing")
	}
	_, err = Load("failed", []func(context.Context) error{func(context.Context) error { return errors.New("failure") }})
	if err == nil {
		t.Fatal("failed warmup accepted")
	}
}

func TestChildBrokerDrainBarrier(t *testing.T) {
	if os.Getenv("PERF_BARRIER_CHILD") == "1" {
		result, err := Load("barrier", []func(context.Context) error{func(context.Context) error { return nil }})
		if err != nil {
			t.Fatal(err)
		}
		phase := os.Getenv("PERF_PHASE_DIR")
		if err := os.WriteFile(filepath.Join(phase, "load-complete"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := Await(ctx, filepath.Join(phase, "broker-drained")); err != nil {
			t.Fatal(err)
		}
		if err := WriteJSON(filepath.Join(phase, "client.json"), result); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Setenv("PERF_BARRIER_CHILD", "1")
	t.Setenv("PERF_RUN", "1")
	t.Setenv("PERF_OUTPUT", t.TempDir())
	t.Setenv("PERF_DURATION", "0.01")
	t.Setenv("PERF_WARMUP", "0.001")
	called := false
	result := ChildWithDrain(t, "barrier", exec.Command(os.Args[0], "-test.run=^TestChildBrokerDrainBarrier$"), func(context.Context) (Backlog, error) { called = true; return Backlog{}, nil })
	if !called || !result.BacklogMeasured || result.BrokerDrain <= 0 {
		t.Fatalf("callback=%v result=%+v", called, result)
	}
}

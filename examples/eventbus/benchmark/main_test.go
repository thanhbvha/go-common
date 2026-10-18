package main

import (
	"testing"
	"time"

	"github.com/thanhbvha/go-common/examples/eventbus/internal/demo"
)

func TestValidateMatrixDimensions(t *testing.T) {
	valid := config{Config: demo.DefaultConfig(), PayloadKB: 1, Concurrency: 10, Workload: "retry10", Messages: 1, Timeout: time.Second}
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

func TestRetryMixAndPercentiles(t *testing.T) {
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
	values := []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond, 5 * time.Millisecond}
	if got := percentile(values, 0.50); got != 3 {
		t.Fatalf("p50 = %v, want 3ms", got)
	}
}

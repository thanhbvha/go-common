package cron

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"github.com/thanhbvha/go-common/cache"
)

func quietOptions() []Option {
	return []Option{WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))}
}
func redisFixture(t *testing.T) (*miniredis.Miniredis, *cache.RedisLock) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), ContextTimeoutEnabled: true, MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	return mr, cache.NewRedisLock(client)
}
func wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}
func invocation(s *Scheduler, at time.Time) <-chan struct{} {
	done := make(chan struct{})
	job := s.cron.Entries()[0].Job.(*scheduledJob)
	go func() { defer close(done); job.run(at) }()
	return done
}
func config(run func(context.Context) error) DistributedConfig {
	return DistributedConfig{JobName: "report", Schedule: "* * * * * *", LockTTL: time.Second, Run: run}
}
func register(t *testing.T, s *Scheduler, cfg DistributedConfig, lock *cache.RedisLock) {
	t.Helper()
	if _, err := s.AddDistributedJob(cfg, lock); err != nil {
		t.Fatal(err)
	}
}

func TestDistributedSlotDeduplication(t *testing.T) {
	_, lock := redisFixture(t)
	var count atomic.Int32
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg := config(func(context.Context) error { count.Add(1); return nil })
	nodes := make([]*Scheduler, 3)
	for i := range nodes {
		nodes[i] = NewScheduler(quietOptions()...)
		register(t, nodes[i], cfg, lock)
	}
	var wg sync.WaitGroup
	for _, s := range nodes {
		wg.Add(1)
		go func() { defer wg.Done(); waitDone := invocation(s, at); <-waitDone }()
	}
	wg.Wait()
	if count.Load() != 1 {
		t.Fatalf("same slot ran %d times", count.Load())
	}
	// A late node must not repeat the slot after the execution lease is released.
	wait(t, invocation(nodes[1], at))
	if count.Load() != 1 {
		t.Fatal("late node repeated slot")
	}
	wait(t, invocation(nodes[2], at.Add(time.Second)))
	if count.Load() != 2 {
		t.Fatal("next slot was suppressed")
	}
}

func TestDistributedRenewalPreventsOverlap(t *testing.T) {
	mr, lock := redisFixture(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var count atomic.Int32
	cfg := config(func(context.Context) error { count.Add(1); close(started); <-release; return nil })
	cfg.Timeout = 50 * time.Millisecond // lease must stay held even after cancellation
	cfg.LockTTL = 120 * time.Millisecond
	cfg.LockTimeout = 15 * time.Millisecond
	a, b := NewScheduler(quietOptions()...), NewScheduler(quietOptions()...)
	register(t, a, cfg, lock)
	register(t, b, cfg, lock)
	at := time.Now()
	done := invocation(a, at)
	wait(t, started)
	// miniredis only expires keys when explicitly advancing time. Advance slower
	// than the renewal period and span several complete lease lifetimes.
	for i := 0; i < 20; i++ {
		time.Sleep(20 * time.Millisecond)
		mr.FastForward(20 * time.Millisecond)
	}
	wait(t, invocation(b, at.Add(time.Second)))
	close(release)
	wait(t, done)
	if count.Load() != 1 {
		t.Fatal("overlapping execution")
	}
}

func TestDistributedLeaseLossCancelsJob(t *testing.T) {
	mr, lock := redisFixture(t)
	started := make(chan struct{})
	canceled := make(chan struct{})
	results := make(chan Result, 2)
	s := NewScheduler(append(quietOptions(), WithObserver(func(r Result) { results <- r }))...)
	cfg := config(func(ctx context.Context) error { close(started); <-ctx.Done(); close(canceled); return ctx.Err() })
	cfg.LockTTL = 150 * time.Millisecond
	register(t, s, cfg, lock)
	done := invocation(s, time.Now())
	wait(t, started)
	mr.Set("cron:lock:report", "new-owner")
	wait(t, canceled)
	wait(t, done)
	result := <-results
	if result.Status != "lease_lost" || !errors.Is(result.Err, ErrLeaseLost) {
		t.Fatalf("unexpected result %+v", result)
	}
	token, _ := mr.Get("cron:lock:report")
	if token != "new-owner" {
		t.Fatal("stale owner released new owner lock")
	}
}

func TestDistributedNamespaceIsolation(t *testing.T) {
	_, lock := redisFixture(t)
	var count atomic.Int32
	cfg := config(func(context.Context) error { count.Add(1); return nil })
	at := time.Now()
	for _, ns := range []string{"service:prod", "service:staging"} {
		s := NewScheduler(append(quietOptions(), WithNamespace(ns))...)
		register(t, s, cfg, lock)
		wait(t, invocation(s, at))
	}
	if count.Load() != 2 {
		t.Fatal("namespaces conflicted")
	}
}

func TestValidation(t *testing.T) {
	_, lock := redisFixture(t)
	valid := config(func(context.Context) error { return nil })
	cases := map[string]func(*DistributedConfig){
		"empty name":          func(c *DistributedConfig) { c.JobName = "" },
		"whitespace name":     func(c *DistributedConfig) { c.JobName = " report " },
		"zero ttl":            func(c *DistributedConfig) { c.LockTTL = 0 },
		"negative ttl":        func(c *DistributedConfig) { c.LockTTL = -time.Second },
		"invalid timeout":     func(c *DistributedConfig) { c.LockTimeout = c.LockTTL },
		"negative timeout":    func(c *DistributedConfig) { c.Timeout = -time.Second },
		"short retention":     func(c *DistributedConfig) { c.DeduplicationTTL = time.Millisecond },
		"nil callback":        func(c *DistributedConfig) { c.Run = nil },
		"invalid schedule":    func(c *DistributedConfig) { c.Schedule = "invalid" },
		"impossible schedule": func(c *DistributedConfig) { c.Schedule = "0 0 0 31 2 *" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := NewScheduler()
			cfg := valid
			mutate(&cfg)
			if _, err := s.AddDistributedJob(cfg, lock); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	s := NewScheduler()
	if _, err := s.AddDistributedJob(valid, nil); err == nil {
		t.Fatal("accepted nil lock")
	}
	if _, err := s.AddDistributedJob(valid, &cache.RedisLock{}); err == nil {
		t.Fatal("accepted nil client")
	}
	register(t, s, valid, lock)
	if _, err := s.AddDistributedJob(valid, lock); err == nil {
		t.Fatal("accepted duplicate name")
	}
	if _, err := s.AddJob(JobConfig{JobName: "nil-callback", Schedule: "* * * * * *"}); err == nil {
		t.Fatal("accepted nil function")
	}
}

func TestLocalResultsAndPanicRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, status string
		run          func(context.Context) error
	}{
		{"error", "failed", func(context.Context) error { return errors.New("business failure") }},
		{"panic", "panic", func(context.Context) error { panic("broken job") }},
		{"timeout", "timeout", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			results := make(chan Result, 2)
			s := NewScheduler(append(quietOptions(), WithObserver(func(r Result) { results <- r }))...)
			_, err := s.AddJob(JobConfig{JobName: tc.name, Schedule: "* * * * * *", Timeout: 20 * time.Millisecond, Run: tc.run})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				wait(t, invocation(s, time.Now()))
				r := <-results
				if r.Status != tc.status || r.Err == nil {
					t.Fatalf("unexpected result %+v", r)
				}
			}
		})
	}
}

func TestLocalOverlapPolicy(t *testing.T) {
	for _, policy := range []OverlapPolicy{SkipIfRunning, AllowOverlap} {
		t.Run(map[OverlapPolicy]string{SkipIfRunning: "skip", AllowOverlap: "allow"}[policy], func(t *testing.T) {
			var count atomic.Int32
			started := make(chan struct{}, 2)
			release := make(chan struct{})
			s := NewScheduler(append(quietOptions(), WithOverlapPolicy(policy))...)
			_, err := s.AddJob(JobConfig{JobName: "overlap", Schedule: "* * * * * *", Run: func(context.Context) error { count.Add(1); started <- struct{}{}; <-release; return nil }})
			if err != nil {
				t.Fatal(err)
			}
			first := invocation(s, time.Now())
			wait(t, started)
			second := invocation(s, time.Now())
			if policy == AllowOverlap {
				wait(t, started)
			} else {
				wait(t, second)
			}
			close(release)
			wait(t, first)
			wait(t, second)
			expected := int32(1)
			if policy == AllowOverlap {
				expected = 2
			}
			if count.Load() != expected {
				t.Fatalf("got %d runs", count.Load())
			}
		})
	}
}

func TestEngineDueTimeAndShutdown(t *testing.T) {
	started := make(chan struct{})
	results := make(chan Result, 1)
	loc := time.FixedZone("UTC+7", 7*3600)
	s := NewScheduler(append(quietOptions(), WithLocation(loc), WithObserver(func(r Result) { results <- r }))...)
	_, err := s.AddJob(JobConfig{JobName: "cancel", Schedule: "* * * * * *", Run: func(ctx context.Context) error { close(started); <-ctx.Done(); return ctx.Err() }})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	s.Start()
	wait(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	r := <-results
	if r.Status != "canceled" || r.ScheduledAt.Nanosecond() != 0 || r.ScheduledAt.Location() != loc {
		t.Fatalf("unexpected result %+v", r)
	}
	if _, err := s.AddJob(JobConfig{JobName: "after-shutdown", Schedule: "* * * * * *", Run: func(context.Context) error { return nil }}); !errors.Is(err, ErrStopped) {
		t.Fatalf("expected ErrStopped got %v", err)
	}
	s.Start()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDeadline(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	s := NewScheduler(quietOptions()...)
	_, err := s.AddJob(JobConfig{JobName: "ignores-cancellation", Schedule: "* * * * * *", Run: func(context.Context) error { close(started); <-release; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	wait(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = s.Shutdown(ctx)
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected error %v", err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseExpiryCancelsWhileRenewalBlocked(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	jobCtx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		maintainLease(ctx, time.Now(), 90*time.Millisecond, 10*time.Millisecond, func(context.Context) (bool, error) { close(entered); <-release; return true, nil }, cancel)
	}()
	wait(t, entered)
	wait(t, jobCtx.Done())
	close(release)
	wait(t, done)
	if !errors.Is(context.Cause(jobCtx), ErrLeaseLost) {
		t.Fatal("expiry did not cancel job")
	}
}

func TestDistributedFailureAndMarkerRetention(t *testing.T) {
	for _, tc := range []struct {
		name, status string
		run          func(context.Context) error
	}{
		{"failed", "failed", func(context.Context) error { return errors.New("write failed") }},
		{"panic", "panic", func(context.Context) error { panic("write panicked") }},
		{"timeout", "timeout", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mr, lock := redisFixture(t)
			results := make(chan Result, 2)
			var count atomic.Int32
			s := NewScheduler(append(quietOptions(), WithObserver(func(r Result) { results <- r }))...)
			cfg := config(func(ctx context.Context) error { count.Add(1); return tc.run(ctx) })
			cfg.Timeout = 20 * time.Millisecond
			register(t, s, cfg, lock)
			at := time.Now()
			wait(t, invocation(s, at))
			r := <-results
			if r.Status != tc.status || r.Err == nil {
				t.Fatalf("unexpected result %+v", r)
			}
			if mr.Exists("cron:lock:report") {
				t.Fatal("execution lease leaked")
			}
			wait(t, invocation(s, at))
			r = <-results
			if r.Status != "skipped_slot" || count.Load() != 1 {
				t.Fatal("failed slot was retried implicitly")
			}
		})
	}
}

func TestDistributedRedisErrors(t *testing.T) {
	for _, renewalFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "acquire", true: "renew"}[renewalFailure], func(t *testing.T) {
			mr, lock := redisFixture(t)
			started := make(chan struct{})
			results := make(chan Result, 1)
			s := NewScheduler(append(quietOptions(), WithObserver(func(r Result) { results <- r }))...)
			cfg := config(func(ctx context.Context) error { close(started); <-ctx.Done(); return ctx.Err() })
			cfg.LockTTL = 150 * time.Millisecond
			register(t, s, cfg, lock)
			if !renewalFailure {
				mr.SetError("ERR simulated outage")
			}
			done := invocation(s, time.Now())
			if renewalFailure {
				wait(t, started)
				mr.SetError("ERR simulated outage")
			}
			wait(t, done)
			r := <-results
			expected := "failed"
			if renewalFailure {
				expected = "lease_lost"
			}
			if r.Status != expected || r.Err == nil {
				t.Fatalf("unexpected result %+v", r)
			}
			if !renewalFailure {
				select {
				case <-started:
					t.Fatal("ran without a lock")
				default:
				}
			}
		})
	}
}

func TestObserverPanicIsolated(t *testing.T) {
	s := NewScheduler(append(quietOptions(), WithObserver(func(Result) { panic("observer failure") }))...)
	var calls atomic.Int32
	if _, err := s.AddJob(JobConfig{JobName: "observer", Schedule: "* * * * * *", Run: func(context.Context) error { calls.Add(1); return nil }}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		wait(t, invocation(s, time.Now()))
	}
	if calls.Load() != 2 {
		t.Fatal("observer panic stopped execution")
	}
}

func TestScheduledJobDeliversDueTimestamp(t *testing.T) {
	parser, err := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow).Parse("*/5 * * * * *")
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan time.Time, 1)
	job := &scheduledJob{schedule: parser, due: make(chan time.Time, 1), run: func(at time.Time) { received <- at }}
	base := time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)
	due := job.Next(base)
	// Simulate the cron loop advancing after dispatch while the job goroutine
	// itself does not run until long after the scheduled instant.
	job.Next(base.Add(12 * time.Second))
	job.Run()
	if got := <-received; !got.Equal(due) {
		t.Fatalf("got %v want %v", got, due)
	}
}

func TestExecutionMetadata(t *testing.T) {
	_, lock := redisFixture(t)
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, distributed := range []bool{false, true} {
		s := NewScheduler(quietOptions()...)
		run := func(ctx context.Context) error {
			execution, ok := ExecutionFromContext(ctx)
			if !ok || execution.JobName != "report" || !execution.ScheduledAt.Equal(at) {
				return errors.New("wrong occurrence metadata")
			}
			return nil
		}
		if distributed {
			register(t, s, config(run), lock)
		} else {
			if _, err := s.AddJob(JobConfig{JobName: "report", Schedule: "* * * * * *", Run: run}); err != nil {
				t.Fatal(err)
			}
		}
		result := make(chan Result, 1)
		s.observer = func(r Result) { result <- r }
		wait(t, invocation(s, at))
		if r := <-result; r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	if _, ok := ExecutionFromContext(context.Background()); ok {
		t.Fatal("metadata exists outside callback")
	}
}

func TestDistributedRejectsRelativeSchedule(t *testing.T) {
	_, lock := redisFixture(t)
	cfg := config(func(context.Context) error { return nil })
	for _, spec := range []string{"@every 1s", "CRON_TZ=UTC @every 1m"} {
		cfg.Schedule = spec
		if _, err := NewScheduler().AddDistributedJob(cfg, lock); err == nil {
			t.Fatal("accepted relative distributed schedule")
		}
	}
	if _, err := NewScheduler().AddJob(JobConfig{JobName: "relative", Schedule: "@every 1s", Run: func(context.Context) error { return nil }}); err != nil {
		t.Fatal("local relative schedules should remain supported", err)
	}
}

func TestShutdownBeforeStart(t *testing.T) {
	s := NewScheduler()
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.Start()
	if _, err := s.AddJob(JobConfig{JobName: "late", Schedule: "* * * * * *", Run: func(context.Context) error { return nil }}); !errors.Is(err, ErrStopped) {
		t.Fatalf("got %v, want ErrStopped", err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentShutdown(t *testing.T) {
	s := NewScheduler()
	s.Start()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := s.Shutdown(ctx); err != nil {
				t.Errorf("concurrent shutdown: %v", err)
			}
		}()
	}
	wg.Wait()
}

func TestCancellationReporting(t *testing.T) {
	businessErr := errors.New("database write failed")
	for _, distributed := range []bool{false, true} {
		mode := "local"
		if distributed {
			mode = "distributed"
		}
		for _, tc := range []struct {
			name, status, level string
			timeout             time.Duration
			callbackError       func(context.Context) error
		}{
			{name: "canceled", status: "canceled", level: "INFO", callbackError: func(ctx context.Context) error { return ctx.Err() }},
			{name: "wrapped cancellation", status: "canceled", level: "INFO", callbackError: func(ctx context.Context) error { return fmt.Errorf("report interrupted: %w", ctx.Err()) }},
			{name: "business failure during shutdown", status: "failed", level: "ERROR", callbackError: func(context.Context) error { return businessErr }},
			{name: "joined business and cancellation", status: "failed", level: "ERROR", callbackError: func(ctx context.Context) error { return errors.Join(businessErr, ctx.Err()) }},
			{name: "timeout", status: "timeout", level: "ERROR", timeout: 20 * time.Millisecond, callbackError: func(ctx context.Context) error { return ctx.Err() }},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				var logs bytes.Buffer
				results := make(chan Result, 1)
				started := make(chan struct{})
				s := NewScheduler(WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))), WithObserver(func(r Result) { results <- r }))
				run := func(ctx context.Context) error { close(started); <-ctx.Done(); return tc.callbackError(ctx) }
				if distributed {
					_, lock := redisFixture(t)
					cfg := config(run)
					cfg.Timeout = tc.timeout
					register(t, s, cfg, lock)
				} else {
					if _, err := s.AddJob(JobConfig{JobName: "report", Schedule: "* * * * * *", Timeout: tc.timeout, Run: run}); err != nil {
						t.Fatal(err)
					}
				}
				done := invocation(s, time.Now())
				wait(t, started)
				if tc.timeout == 0 {
					s.cancel()
				}
				wait(t, done)
				result := <-results
				if result.Status != tc.status {
					t.Fatalf("got status %s want %s", result.Status, tc.status)
				}
				expected := context.Canceled
				if tc.timeout > 0 {
					expected = context.DeadlineExceeded
				}
				if !errors.Is(result.Err, expected) {
					t.Fatalf("missing context cause: %v", result.Err)
				}
				if got := strings.Count(result.Err.Error(), expected.Error()); got != 1 {
					t.Fatalf("context cause appears %d times: %v", got, result.Err)
				}
				if tc.status == "failed" && !errors.Is(result.Err, businessErr) {
					t.Fatal("business failure was hidden")
				}
				var record struct {
					Level  string `json:"level"`
					Status string `json:"status"`
				}
				if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &record); err != nil {
					t.Fatalf("invalid logs %s: %v", logs.String(), err)
				}
				if record.Level != tc.level || record.Status != tc.status {
					t.Fatalf("unexpected log: %s", logs.String())
				}
			})
		}
	}
}

func TestDistributedEngineShutdownCancellation(t *testing.T) {
	mr, lock := redisFixture(t)
	started := make(chan struct{})
	results := make(chan Result, 1)
	s := NewScheduler(append(quietOptions(), WithObserver(func(r Result) { results <- r }))...)
	register(t, s, config(func(ctx context.Context) error { close(started); <-ctx.Done(); return ctx.Err() }), lock)
	s.Start()
	wait(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	result := <-results
	if result.Status != "canceled" || result.Err != context.Canceled {
		t.Fatalf("unexpected result %+v", result)
	}
	if mr.Exists("cron:lock:report") {
		t.Fatal("shutdown leaked execution lease")
	}
}

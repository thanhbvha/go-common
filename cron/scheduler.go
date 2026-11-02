// Package cron provides in-process scheduling with optional Redis leases.
// Distributed jobs must make their business effects idempotent; leases do not
// provide exactly-once delivery or durable recovery of missed schedules.
package cron

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
)

// ErrStopped is returned when registering work after Shutdown.
var ErrStopped = errors.New("cron: scheduler stopped")

// OverlapPolicy controls concurrent invocations of a local job.
type OverlapPolicy int

const (
	SkipIfRunning OverlapPolicy = iota
	AllowOverlap
)

// Result is emitted once per invocation, including skipped and failed attempts.
// Observer callbacks can run concurrently and should return promptly.
type Result struct {
	JobName     string
	ScheduledAt time.Time
	StartedAt   time.Time
	Duration    time.Duration
	Status      string // success, failed, panic, timeout, canceled, lease_lost, skipped_running, skipped_lock, skipped_slot
	Err         error
}

// Option configures a scheduler at construction time.
type Option func(*Scheduler)

// WithLogger supplies the structured execution logger.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Scheduler) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// WithLocation selects the default timezone for calendar expressions.
func WithLocation(location *time.Location) Option {
	return func(s *Scheduler) {
		if location != nil {
			s.location = location
		}
	}
}

// WithNamespace isolates Redis keys by service/environment. Default: "cron".
func WithNamespace(namespace string) Option {
	return func(s *Scheduler) { s.namespace = strings.TrimSpace(namespace) }
}

// WithObserver installs a concurrent, synchronous execution result hook.
func WithObserver(observer func(Result)) Option { return func(s *Scheduler) { s.observer = observer } }

// WithOverlapPolicy selects the overlap policy for local jobs.
func WithOverlapPolicy(policy OverlapPolicy) Option { return func(s *Scheduler) { s.overlap = policy } }

// Scheduler is safe for concurrent registration and lifecycle calls. Shutdown is
// terminal: create a new scheduler to restart. The default local policy skips overlap.
type Scheduler struct {
	cron      *cron.Cron
	mu        sync.Mutex
	stopped   bool
	done      context.Context
	ctx       context.Context
	cancel    context.CancelFunc
	names     map[string]struct{}
	logger    *slog.Logger
	location  *time.Location
	namespace string
	observer  func(Result)
	overlap   OverlapPolicy
}

// NewScheduler accepts six-field expressions (seconds first). UTC is recommended
// via WithLocation; the default timezone is time.Local.
func NewScheduler(options ...Option) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{ctx: ctx, cancel: cancel, names: make(map[string]struct{}), logger: slog.Default(), location: time.Local, namespace: "cron"}
	for _, option := range options {
		if option != nil {
			option(s)
		}
	}
	s.cron = cron.New(cron.WithSeconds(), cron.WithLocation(s.location))
	return s
}

// JobConfig describes a local job. Timeout requires the callback to honor ctx.
type JobConfig struct {
	JobName  string
	Schedule string
	Timeout  time.Duration
	Run      func(context.Context) error
}

// AddJob registers a named local callback with cancellation and error reporting.
func (s *Scheduler) AddJob(cfg JobConfig) (cron.EntryID, error) {
	if cfg.Run == nil {
		return 0, errors.New("cron: nil callback")
	}
	if cfg.Timeout < 0 {
		return 0, errors.New("cron: timeout must not be negative")
	}
	if s.overlap != SkipIfRunning && s.overlap != AllowOverlap {
		return 0, errors.New("cron: invalid overlap policy")
	}
	var busy atomic.Bool
	return s.register(cfg.JobName, cfg.Schedule, func(at time.Time) {
		s.execute(cfg.JobName, at, func(r *Result) {
			if s.overlap == SkipIfRunning {
				if !busy.CompareAndSwap(false, true) {
					r.Status = "skipped_running"
					return
				}
				defer busy.Store(false)
			}
			ctx, cancel := executionContext(s.ctx, cfg.JobName, at, cfg.Timeout)
			defer cancel()
			r.Err = cfg.Run(ctx)
			finishContext(r, ctx)
		})
	})
}

func (s *Scheduler) register(name, spec string, run func(time.Time)) (cron.EntryID, error) {
	if strings.TrimSpace(name) == "" || name != strings.TrimSpace(name) {
		return 0, errors.New("cron: job name must be nonempty and trimmed")
	}
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(spec)
	if err != nil {
		return 0, fmt.Errorf("cron: invalid schedule: %w", err)
	}
	if schedule.Next(time.Now().In(s.location)).IsZero() {
		return 0, errors.New("cron: schedule has no future occurrence")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return 0, ErrStopped
	}
	if _, exists := s.names[name]; exists {
		return 0, fmt.Errorf("cron: duplicate job name %q", name)
	}
	job := &scheduledJob{schedule: schedule, due: make(chan time.Time, 1), run: run}
	id := s.cron.Schedule(job, job)
	s.names[name] = struct{}{}
	return id, nil
}

// scheduledJob passes the actual due time, rather than the goroutine's wall
// clock, to the callback. In robfig v3 each dispatch is followed by Next. A
// terminal Shutdown prevents re-initialization of this schedule on restart.
type scheduledJob struct {
	schedule cron.Schedule
	next     time.Time // accessed only by the cron loop
	due      chan time.Time
	run      func(time.Time)
}

func (j *scheduledJob) Next(now time.Time) time.Time {
	if !j.next.IsZero() {
		j.due <- j.next
	}
	j.next = j.schedule.Next(now)
	return j.next
}
func (j *scheduledJob) Run() { j.run(<-j.due) }

func (s *Scheduler) execute(name string, at time.Time, run func(*Result)) {
	r := Result{JobName: name, ScheduledAt: at, StartedAt: time.Now(), Status: "success"}
	defer func() {
		if p := recover(); p != nil {
			r.Status = "panic"
			r.Err = fmt.Errorf("panic: %v", p)
			s.logger.Error("cron job panic", "job", name, "error", r.Err, "stack", string(debug.Stack()))
		}
		r.Duration = time.Since(r.StartedAt)
		if r.Status == "success" && r.Err != nil {
			r.Status = "failed"
		}
		if r.Status == "canceled" {
			s.logger.Info("cron job finished", "job", name, "status", r.Status, "duration", r.Duration, "error", r.Err)
		} else if r.Err != nil {
			s.logger.Error("cron job finished", "job", name, "status", r.Status, "duration", r.Duration, "error", r.Err)
		} else if strings.HasPrefix(r.Status, "skipped_") {
			s.logger.Debug("cron job skipped", "job", name, "status", r.Status)
		} else {
			s.logger.Info("cron job finished", "job", name, "status", r.Status, "duration", r.Duration)
		}
		if s.observer != nil {
			func() {
				defer func() {
					if p := recover(); p != nil {
						s.logger.Error("cron observer panic", "panic", p)
					}
				}()
				s.observer(r)
			}()
		}
	}()
	run(&r)
}

// Execution identifies a scheduled occurrence for business idempotency keys.
type Execution struct {
	JobName     string
	ScheduledAt time.Time
}
type executionKey struct{}

// ExecutionFromContext returns the occurrence metadata passed to a job callback.
func ExecutionFromContext(ctx context.Context) (Execution, bool) {
	execution, ok := ctx.Value(executionKey{}).(Execution)
	return execution, ok
}
func executionContext(parent context.Context, name string, at time.Time, timeout time.Duration) (context.Context, context.CancelFunc) {
	return jobContext(context.WithValue(parent, executionKey{}, Execution{JobName: name, ScheduledAt: at}), timeout)
}
func jobContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(parent, timeout)
	}
	return context.WithCancel(parent)
}
func finishContext(r *Result, ctx context.Context) {
	if err := ctx.Err(); err != nil {
		cause := context.Cause(ctx)
		// Preserve the callback's wrapping without adding the same context cause
		// twice. Keep independent business errors visible during shutdown.
		if r.Err == nil {
			r.Err = cause
		} else if !errors.Is(r.Err, cause) {
			r.Err = errors.Join(r.Err, cause)
		}
		if !contextErrorOnly(r.Err, err, cause) {
			r.Status = "failed"
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			r.Status = "timeout"
		} else {
			r.Status = "canceled"
		}
	}
}

// contextErrorOnly distinguishes expected cancellation from a joined business
// failure. errors.Is alone would hide a business error joined with ctx.Err().
func contextErrorOnly(err, contextErr, cause error) bool {
	if err == nil {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if !contextErrorOnly(child, contextErr, cause) {
				return false
			}
		}
		return true
	}
	if child := errors.Unwrap(err); child != nil {
		return contextErrorOnly(child, contextErr, cause)
	}
	return errors.Is(err, contextErr) || errors.Is(err, cause)
}

// Start begins scheduling. It is idempotent and has no effect after Shutdown.
func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.stopped {
		s.cron.Start()
	}
}

// Shutdown stops dispatches, cancels job contexts and waits up to ctx's deadline.
// A callback that ignores cancellation cannot be forcibly terminated.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		s.done = s.cron.Stop()
	}
	done := s.done
	s.mu.Unlock()
	s.cancel()
	select {
	case <-done.Done():
		return nil
	default:
	}
	select {
	case <-done.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

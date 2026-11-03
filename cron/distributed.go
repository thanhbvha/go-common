package cron

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
)

// ErrLeaseLost is the job context cause when lease ownership becomes uncertain.
var ErrLeaseLost = errors.New("cron: execution lease lost")

// Lease is one ownership session. Implementations must reject unsafe TTLs,
// honor operation contexts, and prevent an expired owner changing a newer lease.
// An acquired key cannot be reacquired using the same session.
type Lease interface {
	Acquire(context.Context, string, time.Duration) (bool, error)
	Extend(context.Context, string, time.Duration) (bool, error)
	Release(context.Context, string) error
}

// LeaseProvider creates independent ownership identities for each execution.
// See cron/lease/redis for the optional Redis implementation.
type LeaseProvider interface{ NewSession() (Lease, error) }

// DistributedConfig describes a best-effort distributed job. Redis and all
// replicas must use the same namespace, name, schedule and timezone.
type DistributedConfig struct {
	JobName  string
	Schedule string
	// LockTTL is a renewable execution lease, independent of the cron interval.
	// Minimum 30ms; use several seconds or more in production.
	LockTTL time.Duration
	// LockTimeout bounds each Redis operation. Default: min(LockTTL/6, 5s).
	// It must be positive and less than LockTTL/3.
	LockTimeout time.Duration
	// DeduplicationTTL retains markers for completed/attempted schedule slots.
	// Default 24h. After retention expires a sufficiently delayed node can repeat
	// a slot. Markers are not a durable execution history or retry queue.
	DeduplicationTTL time.Duration
	// Timeout bounds the callback context. Zero means no execution deadline.
	Timeout time.Duration
	// Run executes the job. Honor cancellation and return business errors.
	Run func(context.Context) error
}

// AddDistributedJob registers a renewable lease and retained slot marker job.
func (s *Scheduler) AddDistributedJob(cfg DistributedConfig, leases LeaseProvider) (cron.EntryID, error) {
	for _, field := range strings.Fields(cfg.Schedule) {
		if field == "@every" {
			return 0, errors.New("cron: distributed jobs require calendar schedules; @every is relative to each replica startup")
		}
	}
	if strings.TrimSpace(s.namespace) == "" {
		return 0, errors.New("cron: namespace must not be empty")
	}
	if cfg.LockTTL < 30*time.Millisecond {
		return 0, errors.New("cron: LockTTL must be at least 30ms")
	}
	if cfg.LockTimeout == 0 {
		cfg.LockTimeout = min(cfg.LockTTL/6, 5*time.Second)
	}
	if cfg.LockTimeout <= 0 || cfg.LockTimeout >= cfg.LockTTL/3 {
		return 0, errors.New("cron: LockTimeout must be positive and less than LockTTL/3")
	}
	if cfg.DeduplicationTTL == 0 {
		cfg.DeduplicationTTL = 24 * time.Hour
	}
	if cfg.DeduplicationTTL < cfg.LockTTL {
		return 0, errors.New("cron: DeduplicationTTL must be at least LockTTL")
	}
	if cfg.Timeout < 0 {
		return 0, errors.New("cron: timeout must not be negative")
	}
	if cfg.Run == nil {
		return 0, errors.New("cron: nil callback")
	}
	if nilLeaseDependency(leases) {
		return 0, errors.New("cron: lease provider required")
	}
	if lease, err := leases.NewSession(); err != nil {
		return 0, err
	} else if nilLeaseDependency(lease) {
		return 0, errors.New("cron: lease provider returned nil session")
	}
	prefix := url.QueryEscape(s.namespace)
	name := url.QueryEscape(cfg.JobName)
	lockKey := prefix + ":lock:" + name
	var busy atomic.Bool
	return s.register(cfg.JobName, cfg.Schedule, func(at time.Time) {
		s.execute(cfg.JobName, at, func(r *Result) {
			// Also prevent overlap on this replica if a callback ignores cancellation
			// after its lease has been lost.
			if !busy.CompareAndSwap(false, true) {
				r.Status = "skipped_running"
				return
			}
			defer busy.Store(false)
			jobCtx, jobCancel := executionContext(s.ctx, cfg.JobName, at, cfg.Timeout)
			defer jobCancel()
			ctx, cancel := context.WithCancelCause(jobCtx)
			defer cancel(nil)
			defer func() {
				finishContext(r, ctx)
				if errors.Is(context.Cause(ctx), ErrLeaseLost) {
					r.Status = "lease_lost"
				}
			}()
			lease, err := leases.NewSession()
			if err == nil && nilLeaseDependency(lease) {
				err = errors.New("cron: lease provider returned nil session")
			}
			if err != nil {
				r.Err = err
				return
			}
			acquiredAt := time.Now()
			acquired, err := acquire(ctx, lease, lockKey, cfg.LockTTL, cfg.LockTimeout)
			if err != nil {
				r.Err = fmt.Errorf("acquire execution lease: %w", err)
				return
			}
			if !acquired {
				r.Status = "skipped_lock"
				return
			}
			leaseCtx, leaseCancel := context.WithCancel(context.Background())
			leaseDone := make(chan struct{})
			go func() {
				defer close(leaseDone)
				maintainLease(leaseCtx, acquiredAt, cfg.LockTTL, cfg.LockTimeout, func(opCtx context.Context) (bool, error) { return lease.Extend(opCtx, lockKey, cfg.LockTTL) }, cancel)
			}()
			defer func() {
				leaseCancel()
				<-leaseDone
				releaseCtx, releaseCancel := context.WithTimeout(context.Background(), cfg.LockTimeout)
				defer releaseCancel()
				if err := lease.Release(releaseCtx, lockKey); err != nil {
					s.logger.Warn("cron lease release failed", "job", cfg.JobName, "error", err)
				}
			}()
			// Keep this marker after completion, failure or panic. Releasing only the
			// execution lease cannot let a late replica repeat the same schedule slot.
			slotKey := prefix + ":slot:" + name + ":" + strconv.FormatInt(at.UnixNano(), 10)
			claimed, err := acquire(ctx, lease, slotKey, cfg.DeduplicationTTL, cfg.LockTimeout)
			if err != nil {
				r.Err = fmt.Errorf("claim schedule slot: %w", err)
				return
			}
			if !claimed {
				r.Status = "skipped_slot"
				return
			}
			if ctx.Err() != nil {
				return
			}
			// SETNX may have consumed the entire initial lease before the supervisor
			// was scheduled. Fail closed rather than starting with an expired lease.
			if time.Since(acquiredAt) >= cfg.LockTTL {
				cancel(ErrLeaseLost)
				return
			}
			r.Err = cfg.Run(ctx)
		})
	})
}

func acquire(ctx context.Context, lock Lease, key string, ttl, timeout time.Duration) (bool, error) {
	opCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return lock.Acquire(opCtx, key, ttl)
}

type renewal struct {
	started time.Time
	ok      bool
	err     error
}

// maintainLease fails closed on an uncertain renewal. The independent expiry
// timer cancels the job even if a Redis operation ignores its context deadline.
// Only one renewal is in flight. Results use request start time conservatively,
// since a slow response does not extend the confirmed validity window.
func maintainLease(ctx context.Context, acquiredAt time.Time, ttl, timeout time.Duration, extend func(context.Context) (bool, error), cancel context.CancelCauseFunc) {
	validUntil := acquiredAt.Add(ttl)
	expiry := time.NewTimer(max(time.Until(validUntil), 0))
	defer expiry.Stop()
	tick := time.NewTicker(ttl / 3)
	defer tick.Stop()
	results := make(chan renewal, 1)
	pending := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-expiry.C:
			cancel(ErrLeaseLost)
			return
		case <-tick.C:
			if !time.Now().Before(validUntil) {
				cancel(ErrLeaseLost)
				return
			}
			if pending {
				continue
			}
			pending = true
			go func() {
				opCtx, opCancel := context.WithTimeout(ctx, timeout)
				defer opCancel()
				started := time.Now()
				ok, err := extend(opCtx)
				results <- renewal{started: started, ok: ok, err: err}
			}()
		case result := <-results:
			if ctx.Err() != nil {
				return
			}
			pending = false
			if result.err != nil || !result.ok || !time.Now().Before(validUntil) || time.Since(result.started) >= timeout {
				cancel(ErrLeaseLost)
				return
			}
			validUntil = result.started.Add(ttl)
			expiry.Reset(max(time.Until(validUntil), 0))
		}
	}
}

func nilLeaseDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Chan, reflect.Func, reflect.Map, reflect.Slice, reflect.Interface:
		return v.IsNil()
	}
	return false
}

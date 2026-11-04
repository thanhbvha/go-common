package outbox

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
)

// Dispatcher is single-use and borrows both dependencies. Run's context owns
// runtime cancellation; Drain stops intake without canceling accepted work.
// A timeout/cancellation leaves unsettled claims recoverable after lease expiry.
type Dispatcher struct {
	store           Store
	publisher       Publisher
	cfg             Config
	mu              sync.Mutex
	started, closed bool
	stop            chan struct{}
	done            chan struct{}
	cancel          context.CancelFunc
}

func NewDispatcher(store Store, publisher Publisher, cfg Config) (*Dispatcher, error) {
	if store == nil || publisher == nil {
		return nil, messaging.ErrInvalidConfig
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Dispatcher{store: store, publisher: publisher, cfg: cfg, stop: make(chan struct{}), done: make(chan struct{})}, nil
}
func (d *Dispatcher) Run(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return messaging.ErrClosed
	}
	if d.started {
		d.mu.Unlock()
		return messaging.ErrAlreadyRunning
	}
	d.started = true
	workCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	d.mu.Unlock()
	defer func() { cancel(); d.mu.Lock(); d.closed = true; close(d.done); d.mu.Unlock() }()
	// Each worker claims exactly one row at a time: no leased waiting queue.
	errorsCh := make(chan error, d.cfg.Concurrency)
	var wg sync.WaitGroup
	for i := 0; i < d.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.worker(workCtx); err != nil {
				errorsCh <- err
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}
func (d *Dispatcher) Drain(ctx context.Context) error {
	d.mu.Lock()
	if !d.closed {
		d.closed = true
		close(d.stop)
		if !d.started {
			close(d.done)
		}
	}
	done, cancel := d.done, d.cancel
	d.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		if cancel != nil {
			cancel()
		}
		return ctx.Err()
	}
}
func (d *Dispatcher) wait(ctx context.Context, delay time.Duration) bool {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-d.stop:
		return false
	case <-t.C:
		return true
	}
}
func (d *Dispatcher) worker(ctx context.Context) (result error) {
	completed := false
	defer func() {
		if !completed {
			_ = recover()
			result = messaging.ErrInfrastructurePanic
		}
	}()
	result = d.poll(ctx)
	completed = true
	return result
}
func (d *Dispatcher) poll(ctx context.Context) error {
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-d.stop:
			return nil
		default:
		}
		claimCtx, cancel := context.WithTimeout(ctx, d.cfg.SettlementTimeout)
		claims, err := d.store.Claim(claimCtx, ClaimRequest{Limit: 1, MaxAttempts: d.cfg.MaxAttempts, Lease: d.cfg.Lease})
		cancel()
		if err != nil {
			messaging.Observe(ctx, d.cfg.Observer, messaging.Observation{Operation: "claim", Outcome: "error"})
			if errors.Is(err, messaging.ErrSchema) || errors.Is(err, messaging.ErrInvalidConfig) {
				return err
			}
			failures++
			if !d.wait(ctx, RetryDelay(failures, d.cfg.RetryMin, d.cfg.RetryMax)) {
				return nil
			}
			continue
		}
		failures = 0
		if len(claims) > 1 {
			return messaging.ErrInvalidConfig
		}
		if len(claims) == 0 {
			if !d.wait(ctx, d.cfg.PollInterval) {
				return nil
			}
			continue
		}
		d.deliver(ctx, claims[0])
	}
}
func (d *Dispatcher) deliver(ctx context.Context, claim Claim) {
	started := time.Now()
	pubCtx, cancel := context.WithTimeout(ctx, d.cfg.PublishTimeout)
	err := d.publisher.Publish(pubCtx, claim.Message)
	if err == nil {
		err = pubCtx.Err()
	}
	cancel()
	outcome := "published"
	if err != nil {
		outcome = "retry"
	}
	messaging.Observe(ctx, d.cfg.Observer, messaging.Observation{Operation: "publish", Outcome: outcome, Duration: time.Since(started), Attempt: claim.Attempt})
	// Settlement has its own bounded cleanup budget even after publish timeout.
	settleCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), d.cfg.SettlementTimeout)
	defer stop()
	var settled error
	if err == nil {
		settled = d.store.MarkPublished(settleCtx, claim)
	} else if claim.Attempt >= d.cfg.MaxAttempts {
		outcome = "dead"
		settled = d.store.Quarantine(settleCtx, claim, "publication_failed")
	} else {
		settled = d.store.Retry(settleCtx, claim, RetryDelay(claim.Attempt, d.cfg.RetryMin, d.cfg.RetryMax), "publication_failed")
	}
	if errors.Is(settled, messaging.ErrLeaseLost) {
		outcome = "lease_lost"
	} else if settled != nil {
		outcome = "error"
	}
	messaging.Observe(ctx, d.cfg.Observer, messaging.Observation{Operation: "settle", Outcome: outcome, Attempt: claim.Attempt})
}

package kafka

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
)

// rebalanceController bridges franz-go's group callbacks with the offset
// coordinator. BlockRebalanceOnPoll keeps the current assignment valid while a
// bounded batch drains; blocked is the early signal that stops further intake.
type rebalanceController struct {
	mu          sync.RWMutex
	coordinator *offsetCoordinator
	blocked     atomic.Bool
	signal      chan struct{}
	onBlocked   func()
}

func newRebalanceController() *rebalanceController {
	return &rebalanceController{signal: make(chan struct{}, 1)}
}

func (r *rebalanceController) bind(coordinator *offsetCoordinator) {
	r.mu.Lock()
	r.coordinator = coordinator
	r.mu.Unlock()
	coordinator.trackOwnership()
}

func (r *rebalanceController) options() []kgo.Opt {
	return []kgo.Opt{
		kgo.OnPartitionsCallbackBlocked(func(context.Context, *kgo.Client) {
			r.block()
		}),
		kgo.OnPartitionsAssigned(func(ctx context.Context, _ *kgo.Client, partitions map[string][]int32) {
			_ = r.assigned(ctx, partitions)
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, _ *kgo.Client, partitions map[string][]int32) {
			_ = r.revoked(ctx, partitions, false)
		}),
		kgo.OnPartitionsLost(func(ctx context.Context, _ *kgo.Client, partitions map[string][]int32) {
			_ = r.revoked(ctx, partitions, true)
		}),
	}
}

func (r *rebalanceController) block() {
	r.blocked.Store(true)
	if r.onBlocked != nil {
		r.onBlocked()
	}
	select {
	case r.signal <- struct{}{}:
	default:
	}
}

func (r *rebalanceController) assigned(ctx context.Context, partitions map[string][]int32) error {
	err := r.withCoordinator(func(c *offsetCoordinator) error { return c.assign(ctx, partitions) })
	r.unblock()
	return err
}

func (r *rebalanceController) revoked(ctx context.Context, partitions map[string][]int32, lost bool) error {
	err := r.withCoordinator(func(c *offsetCoordinator) error { return c.revoke(ctx, partitions, lost) })
	r.unblock()
	return err
}

func (r *rebalanceController) withCoordinator(fn func(*offsetCoordinator) error) error {
	r.mu.RLock()
	coordinator := r.coordinator
	r.mu.RUnlock()
	if coordinator != nil {
		return fn(coordinator)
	}
	return nil
}

func (r *rebalanceController) unblock() {
	r.blocked.Store(false)
	select {
	case <-r.signal:
	default:
	}
}

func (r *rebalanceController) requested() bool {
	return r != nil && r.blocked.Load()
}

func (r *rebalanceController) requestedSignal() <-chan struct{} {
	if r == nil {
		return nil
	}
	return r.signal
}

package main

import (
	"context"
	"sync"

	"github.com/thanhbvha/go-common/v2/websocket/routing"
)

// Stop publication admission before draining EventBus consumers. Checking only
// HTTP readiness would leave existing WebSocket connections able to publish.
type commandIntake struct {
	next     routing.CommandPublisher
	mu       sync.Mutex
	stopped  bool
	inflight sync.WaitGroup
	stopOnce sync.Once
	done     chan struct{}
}

func (p *commandIntake) PublishCommand(ctx context.Context, c routing.BusinessCommand) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return routing.ErrUnavailable
	}
	p.inflight.Add(1)
	p.mu.Unlock()
	defer p.inflight.Done()
	return p.next.PublishCommand(ctx, c)
}
func (p *commandIntake) stop(ctx context.Context) error {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.stopped = true
		p.mu.Unlock()
		go func() { p.inflight.Wait(); close(p.done) }()
	})
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

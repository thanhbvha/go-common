package mongodb

import (
	"context"
	"sync"
)

// contextGate serializes lifecycle changes without ignoring cancellation while waiting.
type contextGate struct {
	once  sync.Once
	token chan struct{}
}

func (g *contextGate) Lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.once.Do(func() { g.token = make(chan struct{}, 1); g.token <- struct{}{} })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.token:
		if err := ctx.Err(); err != nil {
			g.Unlock()
			return err
		}
		return nil
	}
}
func (g *contextGate) Unlock() { g.token <- struct{}{} }

// All generation fields are protected by Manager.mu. A timed-out drain retains
// the generation and its clients so DisconnectAll can safely be retried.
type transactionGeneration struct {
	active  int
	closing bool
	drained chan struct{}
}

func (m *Manager) releaseTransaction(g *transactionGeneration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g.active--
	if g.closing && g.active == 0 {
		close(g.drained)
	}
}

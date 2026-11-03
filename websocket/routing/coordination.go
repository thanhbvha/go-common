package routing

import (
	"context"
	"sync"
)

// contextMutex supports cancellation while waiting, without a waiter goroutine.
type contextMutex struct {
	once sync.Once
	gate chan struct{}
}

func (m *contextMutex) init() { m.once.Do(func() { m.gate = make(chan struct{}, 1) }) }
func (m *contextMutex) lock(ctx context.Context) error {
	return m.lockUntil(ctx, nil)
}

func (m *contextMutex) lockUntil(ctx context.Context, stop <-chan struct{}) error {
	m.init()
	if err := ctx.Err(); err != nil {
		return err
	}
	acquired := m.tryLock()
	if !acquired {
		select {
		case m.gate <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		case <-stop:
			return context.Canceled
		}
	}
	if err := ctx.Err(); err != nil {
		m.unlock()
		return err
	}
	select {
	case <-stop:
		m.unlock()
		return context.Canceled
	default:
	}
	return nil
}
func (m *contextMutex) tryLock() bool {
	m.init()
	select {
	case m.gate <- struct{}{}:
		return true
	default:
		return false
	}
}
func (m *contextMutex) unlock() { <-m.gate }

type sessionGate struct {
	mutex contextMutex
	refs  int
}
type sessionGates struct {
	mu      sync.Mutex
	entries map[string]*sessionGate
}

func (g *sessionGates) reference(key string) *sessionGate {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.entries == nil {
		g.entries = make(map[string]*sessionGate)
	}
	e := g.entries[key]
	if e == nil {
		e = &sessionGate{}
		g.entries[key] = e
	}
	e.refs++
	return e
}
func (g *sessionGates) unreference(key string, e *sessionGate) {
	g.mu.Lock()
	defer g.mu.Unlock()
	e.refs--
	if e.refs == 0 {
		delete(g.entries, key)
	}
}
func (g *sessionGates) acquire(ctx context.Context, key string) (func(), error) {
	e := g.reference(key)
	if err := e.mutex.lock(ctx); err != nil {
		g.unreference(key, e)
		return nil, err
	}
	return func() { e.mutex.unlock(); g.unreference(key, e) }, nil
}
func (g *sessionGates) tryAcquire(key string) (func(), bool) {
	e := g.reference(key)
	if !e.mutex.tryLock() {
		g.unreference(key, e)
		return nil, false
	}
	return func() { e.mutex.unlock(); g.unreference(key, e) }, true
}

func (g *Gateway) registryPermit(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case g.registrySlots <- struct{}{}:
		return func() { <-g.registrySlots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// operationGroup closes admission atomically with tracking in-flight work.
// WaitGroup alone cannot safely race Add with a shutdown Wait at zero.
type operationGroup struct {
	mu      sync.Mutex
	active  int
	stopped bool
	done    chan struct{}
}

func (g *operationGroup) begin() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return false
	}
	g.active++
	return true
}
func (g *operationGroup) end() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
	if g.stopped && g.active == 0 {
		close(g.done)
	}
}
func (g *operationGroup) stop() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.stopped {
		g.stopped = true
		g.done = make(chan struct{})
		if g.active == 0 {
			close(g.done)
		}
	}
	return g.done
}

func (g *Gateway) operationContext(parent context.Context) (context.Context, func(), error) {
	if !g.operations.begin() {
		return nil, nil, ErrUnavailable
	}
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(g.ctx, cancel)
	return ctx, func() { stop(); cancel(); g.operations.end() }, nil
}

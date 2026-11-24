package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrHandlerCapacity   = errors.New("websocket handler supervision capacity reached")
	ErrSupervisorStopped = errors.New("websocket handler supervisor stopped")
)

type HandlerSupervisorStats struct {
	Capacity    int
	Running     int64
	Quarantined int64
	Rejected    uint64
	Accepting   bool
}

type HandlerSupervisor struct {
	permits     chan struct{}
	mu          sync.RWMutex
	accepting   bool
	running     atomic.Int64
	quarantined atomic.Int64
	rejected    atomic.Uint64
}

func NewHandlerSupervisor(capacity int) *HandlerSupervisor {
	if capacity <= 0 {
		capacity = 1
	}
	return &HandlerSupervisor{permits: make(chan struct{}, capacity), accepting: true}
}

// ErrHandlerScopeCapacity reports that one scope (a connection) already has its
// maximum number of live handler goroutines, which usually means earlier
// handlers ignored cancellation and are still running.
var ErrHandlerScopeCapacity = errors.New("websocket handler scope capacity reached")

// HandlerScope bounds the live handler goroutines attributable to one owner,
// such as a connection. A goroutine that outlives its timeout still counts, so
// a single owner cannot drain the supervisor's shared capacity with handlers
// that never observe their context.
type HandlerScope struct {
	limit   int32
	running atomic.Int32
}

// NewHandlerScope creates a scope allowing at most limit live goroutines.
func NewHandlerScope(limit int) *HandlerScope {
	if limit <= 0 {
		limit = 1
	}
	return &HandlerScope{limit: int32(limit)}
}

func (h *HandlerScope) acquire() bool {
	for {
		current := h.running.Load()
		if current >= h.limit {
			return false
		}
		if h.running.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

// Running reports the live goroutines currently held by the scope.
func (h *HandlerScope) Running() int { return int(h.running.Load()) }

func (s *HandlerSupervisor) Run(ctx context.Context, run func(context.Context) error) error {
	return s.RunScoped(ctx, nil, run)
}

// RunScoped is Run with an additional per-owner goroutine bound. A nil scope
// applies only the shared capacity.
func (s *HandlerSupervisor) RunScoped(ctx context.Context, scope *HandlerScope, run func(context.Context) error) error {
	if run == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.RLock()
	if !s.accepting {
		s.mu.RUnlock()
		s.rejected.Add(1)
		return ErrSupervisorStopped
	}
	if scope != nil && !scope.acquire() {
		s.mu.RUnlock()
		s.rejected.Add(1)
		return ErrHandlerScopeCapacity
	}
	select {
	case s.permits <- struct{}{}:
		s.running.Add(1)
		s.mu.RUnlock()
	default:
		s.mu.RUnlock()
		if scope != nil {
			scope.running.Add(-1)
		}
		s.rejected.Add(1)
		return ErrHandlerCapacity
	}

	type invocationState struct {
		sync.Mutex
		completed   bool
		quarantined bool
	}
	state := &invocationState{}
	type invocationResult struct {
		err        error
		panicValue any
	}
	result := make(chan invocationResult, 1)
	go func() {
		outcome := invocationResult{}
		defer func() {
			outcome.panicValue = recover()
			state.Lock()
			state.completed = true
			if state.quarantined {
				s.quarantined.Add(-1)
			}
			state.Unlock()
			s.running.Add(-1)
			<-s.permits
			if scope != nil {
				scope.running.Add(-1)
			}
			result <- outcome
		}()
		outcome.err = run(ctx)
	}()

	select {
	case outcome := <-result:
		if outcome.panicValue != nil {
			panic(outcome.panicValue)
		}
		return outcome.err
	case <-ctx.Done():
		state.Lock()
		if state.completed {
			state.Unlock()
			outcome := <-result
			if outcome.panicValue != nil {
				panic(outcome.panicValue)
			}
			return outcome.err
		}
		state.quarantined = true
		s.quarantined.Add(1)
		state.Unlock()
		return ctx.Err()
	}
}

func (s *HandlerSupervisor) Stop() {
	s.mu.Lock()
	s.accepting = false
	s.mu.Unlock()
}

func (s *HandlerSupervisor) Stats() HandlerSupervisorStats {
	s.mu.RLock()
	accepting := s.accepting
	s.mu.RUnlock()
	return HandlerSupervisorStats{
		Capacity: cap(s.permits), Running: s.running.Load(), Quarantined: s.quarantined.Load(),
		Rejected: s.rejected.Load(), Accepting: accepting,
	}
}

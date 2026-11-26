package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func stuckHandler(gate chan struct{}) func(context.Context) error {
	return func(context.Context) error { <-gate; return nil }
}

func TestHandlerScopeStopsOneOwnerDrainingSharedCapacity(t *testing.T) {
	supervisor := NewHandlerSupervisor(8)
	attacker, bystander := NewHandlerScope(2), NewHandlerScope(2)
	gate := make(chan struct{})
	defer close(gate)

	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()
		if err := supervisor.RunScoped(ctx, attacker, stuckHandler(gate)); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("attacker handler %d: %v", i, err)
		}
	}
	if err := supervisor.RunScoped(context.Background(), attacker, func(context.Context) error { return nil }); !errors.Is(err, ErrHandlerScopeCapacity) {
		t.Fatalf("attacker beyond its scope = %v, want ErrHandlerScopeCapacity", err)
	}
	if err := supervisor.RunScoped(context.Background(), bystander, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("bystander starved by attacker: %v", err)
	}
	if got := supervisor.Stats().Running; got != 2 {
		t.Fatalf("running = %d, want only the 2 stuck attacker handlers", got)
	}
}

func TestHandlerScopeReleasesWhenHandlerFinishes(t *testing.T) {
	supervisor := NewHandlerSupervisor(4)
	scope := NewHandlerScope(1)
	gate := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := supervisor.RunScoped(ctx, scope, stuckHandler(gate)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if scope.Running() != 1 {
		t.Fatalf("scope running = %d after timeout, want 1 (goroutine still alive)", scope.Running())
	}
	close(gate)
	waitForCondition(t, func() bool { return scope.Running() == 0 })
	if err := supervisor.RunScoped(context.Background(), scope, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("scope not reusable after handler exit: %v", err)
	}
}

func TestHandlerScopeReleasedWhenSharedCapacityRejects(t *testing.T) {
	supervisor := NewHandlerSupervisor(1)
	scope := NewHandlerScope(5)
	gate := make(chan struct{})
	defer close(gate)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	_ = supervisor.RunScoped(ctx, NewHandlerScope(1), stuckHandler(gate))

	if err := supervisor.RunScoped(context.Background(), scope, func(context.Context) error { return nil }); !errors.Is(err, ErrHandlerCapacity) {
		t.Fatalf("err = %v, want ErrHandlerCapacity", err)
	}
	if scope.Running() != 0 {
		t.Fatalf("scope leaked a slot on shared-capacity rejection: %d", scope.Running())
	}
}

func TestConnectionsGetIndependentHandlerScopes(t *testing.T) {
	running, shard, _ := newRunningTestConnection(t, newMockConn())
	t.Cleanup(func() { running.Close("test complete"); waitConnection(t, running) })
	a := NewConnection(newMockConn(), shard, "a", "s", "", "", "")
	b := NewConnection(newMockConn(), shard, "b", "s", "", "", "")
	if a.handlerScope == nil || a.handlerScope == b.handlerScope {
		t.Fatal("connections must not share a handler scope")
	}
	if a.handlerScope.limit != int32(shard.config.MaxLiveHandlersPerConnection) {
		t.Fatalf("scope limit = %d", a.handlerScope.limit)
	}
}

package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHandlerSupervisorBoundsNonCooperativeHandlers(t *testing.T) {
	supervisor := NewHandlerSupervisor(2)
	gate := make(chan struct{})
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if err := supervisor.Run(ctx, func(context.Context) error { <-gate; return nil }); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("timeout error = %v", err)
		}
	}
	if err := supervisor.Run(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrHandlerCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
	stats := supervisor.Stats()
	if stats.Running != 2 || stats.Quarantined != 2 || stats.Rejected != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	close(gate)
	waitForCondition(t, func() bool { return supervisor.Stats().Running == 0 })
}

func TestHandlerSupervisorStopRejectsNewWork(t *testing.T) {
	supervisor := NewHandlerSupervisor(1)
	supervisor.Stop()
	if err := supervisor.Run(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrSupervisorStopped) {
		t.Fatalf("stop error = %v", err)
	}
}

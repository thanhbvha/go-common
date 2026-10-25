package core

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestManagerShutdownCompletesActiveConnections(t *testing.T) {
	manager := NewManager()
	transport := newMockConn()
	connectionDone := make(chan error, 1)
	go func() {
		connectionDone <- manager.HandleConnection(transport, defaultShardName, "shutdown-user", "127.0.0.1", "request")
	}()
	waitForCondition(t, func() bool { return manager.GetTotalConnections() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if manager.State() != StateStopped || manager.GetTotalConnections() != 0 {
		t.Fatalf("state=%v connections=%d", manager.State(), manager.GetTotalConnections())
	}
	select {
	case err := <-connectionDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("connection handler did not complete")
	}
}

func TestManagerConcurrentShutdownIsIdempotent(t *testing.T) {
	manager := NewManager()
	var wg sync.WaitGroup
	errors := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			errors <- manager.Shutdown(ctx)
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestManagerShutdownHonorsContext(t *testing.T) {
	manager := NewManager()
	gate := make(chan struct{})
	if err := manager.eventExecutor.Submit(context.Background(), "blocked", func(context.Context) error {
		<-gate
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := manager.Shutdown(ctx); err != context.DeadlineExceeded {
		t.Fatalf("shutdown error = %v", err)
	}
	close(gate)
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

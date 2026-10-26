package eventbus

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/thanhbvha/go-common/eventbus/driver/memory"
	"github.com/thanhbvha/go-common/eventbus/registry"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestEngine_C4_StateMachine(t *testing.T) {
	mem, _ := registry.Get("memory")
	cfg := DefaultConfig()
	cfg.Driver = "memory"

	bus, err := Open(cfg, WithTransport(mem))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Declaration and handler attachment before start should work.
	err = bus.DeclareTask("test")
	if err != nil {
		t.Fatalf("DeclareTask before start failed: %v", err)
	}
	err = bus.HandleTask("test", func(ctx context.Context, job Job) error { return nil })
	if err != nil {
		t.Fatalf("HandleTask before start failed: %v", err)
	}

	// Start the bus
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = bus.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Declaration after start should fail (Bug C4).
	err = bus.DeclareTask("test2")
	if !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("Expected ErrAlreadyStarted, got %v", err)
	}

	// Second start should fail
	err = bus.Start(ctx)
	if !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("Expected ErrAlreadyStarted on second start, got %v", err)
	}

	bus.Stop(context.Background())
	t.Log("Bug C4 fixed: state machine prevents invalid state transitions.")
}

func TestEngine_C10_Concurrency(t *testing.T) {
	mem, _ := registry.Get("memory")
	cfg := DefaultConfig()
	cfg.Concurrency = 2 // Only 2 concurrent workers

	bus, _ := Open(cfg, WithTransport(mem))

	var active int32
	var maxActive int32

	if err := bus.DeclareTask("test-conc"); err != nil {
		t.Fatal(err)
	}
	if err := bus.HandleTask("test-conc", func(ctx context.Context, job Job) error {
		cur := atomic.AddInt32(&active, 1)

		for {
			m := atomic.LoadInt32(&maxActive)
			if cur > m {
				if atomic.CompareAndSwapInt32(&maxActive, m, cur) {
					break
				}
			} else {
				break
			}
		}

		time.Sleep(100 * time.Millisecond) // hold the worker
		atomic.AddInt32(&active, -1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus.Start(ctx)

	// Enqueue 5 jobs
	for i := 0; i < 5; i++ {
		bus.Enqueue(ctx, "test-conc", "payload")
	}

	time.Sleep(500 * time.Millisecond)
	bus.Stop(context.Background())

	if maxActive > 2 {
		t.Fatalf("Bug C10 NOT fixed! Concurrency breached: expected max 2, got %d", maxActive)
	}
	t.Logf("Bug C10 fixed: Concurrency limit respected (max active = %d)", maxActive)
}

func TestEngine_C1_ProcessTimeout(t *testing.T) {
	mem, _ := registry.Get("memory")
	cfg := DefaultConfig()
	cfg.ProcessTimeout = 100 * time.Millisecond

	bus, _ := Open(cfg, WithTransport(mem))

	done := make(chan struct{})
	if err := bus.DeclareTask("test-timeout"); err != nil {
		t.Fatal(err)
	}
	if err := bus.HandleTask("test-timeout", func(ctx context.Context, job Job) error {
		select {
		case <-time.After(1 * time.Second):
			t.Errorf("Handler should have been canceled")
			return nil
		case <-ctx.Done():
			close(done)
			return ctx.Err()
		}
	}); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	bus.Start(ctx)
	bus.Enqueue(ctx, "test-timeout", "payload")

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatalf("Timeout was not enforced")
	}

	bus.Stop(context.Background())
	t.Log("Bug C1 fixed: ProcessTimeout cancels handler context correctly.")
}

func TestEngine_C14_PoisonMessage(t *testing.T) {
	mem, _ := registry.Get("memory")
	cfg := DefaultConfig()
	cfg.MaxAttempts = 3
	cfg.RetryPolicy = ConstantBackoff{Delay: 10 * time.Millisecond}

	b, _ := Open(cfg, WithTransport(mem))

	attempts := int32(0)
	if err := b.DeclareTask("test-poison"); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleTask("test-poison", func(ctx context.Context, job Job) error {
		atomic.AddInt32(&attempts, 1)
		// panicking to simulate poison message
		panic("boom")
	}); err != nil {
		t.Fatal(err)
	}

	// Add recovery middleware to the test so panic is converted to error
	b.(*bus).engine.handlers["test-poison"] = Handle(
		func(ctx context.Context, job Job, payload any) error {
			atomic.AddInt32(&attempts, 1)
			panic("boom")
		},
		Recoverer(cfg.Logger),
	)

	ctx := context.Background()
	b.Start(ctx)
	b.Enqueue(ctx, "test-poison", "payload")

	// Wait for the message to be tried MaxAttempts times before shutdown;
	// administration is intentionally closed once Stop begins.
	var stats transport.TopologyStats
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats, _ = b.TopologyStats(ctx, TaskRef("test-poison"))
		if stats.DLQ == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if stats.DLQ != 1 {
		t.Fatalf("Bug C14 NOT fixed! Expected 1 job in DLQ, got %d", stats.DLQ)
	}
	b.Stop(ctx)

	actualAttempts := atomic.LoadInt32(&attempts)
	if actualAttempts > int32(cfg.MaxAttempts) {
		t.Fatalf("Bug C14 NOT fixed! Handler executed %d times, max %d", actualAttempts, cfg.MaxAttempts)
	}

	t.Logf("Bug C14 fixed: Poison message caught by MaxAttempts (%d) and sent to DLQ.", cfg.MaxAttempts)
}

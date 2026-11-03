package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/eventbus"
)

func TestBusRecoversAfterDLQCapacityIsFreed(t *testing.T) {
	backend := New()
	cfg := eventbus.DefaultConfig()
	cfg.Concurrency, cfg.Prefetch = 1, 1
	bus, err := eventbus.Open(cfg, eventbus.WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := bus.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	const task = "capacity-recovery"
	if err := bus.DeclareTask(task, eventbus.WithOption("memory.max_dlq_entries", 1)); err != nil {
		t.Fatal(err)
	}
	calls := make(chan struct{}, 10)
	if err := bus.HandleTask(task, func(context.Context, eventbus.Job) error {
		calls <- struct{}{}
		return eventbus.MarkPermanent(errors.New("synthetic"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := bus.Enqueue(context.Background(), task, 1); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case <-calls:
		case <-time.After(time.Second):
			t.Fatal("handler did not run")
		}
	}
	ref := eventbus.TaskRef(task)
	// Wait until the second source has failed settlement and session cleanup
	// has returned it to ready, rather than purging before it hits capacity.
	deadline := time.Now().Add(2 * time.Second)
	for {
		stats, err := bus.TopologyStats(context.Background(), ref)
		if err != nil {
			t.Fatal(err)
		}
		if stats.DLQ == 1 && stats.Pending == 1 && stats.InFlight == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("failed source was not recovered: %+v", stats)
		}
		time.Sleep(time.Millisecond)
	}
	if err := bus.PurgeDeadLetters(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		stats, err := bus.TopologyStats(context.Background(), ref)
		if err != nil {
			t.Fatal(err)
		}
		if stats.DLQ == 1 && stats.Pending == 0 && stats.InFlight == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("source did not settle after capacity was freed: %+v", stats)
		}
		time.Sleep(time.Millisecond)
	}
}

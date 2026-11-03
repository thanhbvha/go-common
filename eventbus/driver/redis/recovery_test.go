package redis

import (
	"context"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/eventbus"
)

func TestBusRecoversIntakeCapacityAfterAckResponseLoss(t *testing.T) {
	backend, rc := delayedBackend(t)
	if err := ackOwnedScript.Load(context.Background(), rc).Err(); err != nil {
		t.Fatal(err)
	}
	// Two lost responses exhaust the old session's two permits even though
	// Redis has already ACKed both sources. Neither source can be reclaimed.
	hooks := []*postSuccessErrorHook{
		{commands: map[string]bool{"evalsha": true}, scriptHashes: map[string]bool{ackOwnedScript.Hash(): true}},
		{commands: map[string]bool{"evalsha": true}, scriptHashes: map[string]bool{ackOwnedScript.Hash(): true}},
	}
	for _, hook := range hooks {
		rc.AddHook(hook)
	}
	cfg := eventbus.DefaultConfig()
	cfg.Concurrency, cfg.Prefetch = 1, 1
	bus, err := eventbus.Open(cfg, eventbus.WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := bus.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	const task = "ack-response-recovery"
	if err := bus.DeclareTask(task, eventbus.WithOption("redis.block", 10*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	calls := make(chan struct{}, 4)
	if err := bus.HandleTask(task, func(context.Context, eventbus.Job) error { calls <- struct{}{}; return nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := bus.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if err := bus.Enqueue(ctx, task, 1); err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		select {
		case <-calls:
		case <-ctx.Done():
			t.Fatal("consumer stalled after ambiguous ACKs")
		}
	}
	for _, hook := range hooks {
		if !hook.fired.Load() {
			t.Fatal("ACK response loss was not injected")
		}
	}
	for {
		stats, err := bus.TopologyStats(ctx, eventbus.TaskRef(task))
		if err != nil {
			t.Fatal(err)
		}
		if stats.Pending == 0 && stats.InFlight == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("sources remain unsettled: %+v", stats)
		case <-time.After(time.Millisecond):
		}
	}
}

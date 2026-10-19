package redis

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

// DS-20/22/40: claiming keeps the original stream entry, pending state and
// broker execution history; no republish or premature ACK is permitted.
func TestReclaimPreservesSourceAndDeliveryHistory(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{JobType: "reclaim", Options: map[string]any{"redis.ack_wait": time.Millisecond}}
	d := pendingDelivery(t, backend, rc, topo)
	for want := 2; want <= 3; want++ {
		if err := rc.Do(ctx, "XCLAIM", d.stream, d.group, "old", 0, d.xmsgID, "IDLE", time.Hour.Milliseconds()).Err(); err != nil {
			t.Fatal(err)
		}
		// The preparatory XCLAIM itself increments history, too.
		before, err := rc.XPendingExt(ctx, &goredis.XPendingExtArgs{Stream: d.stream, Group: d.group, Start: d.xmsgID, End: d.xmsgID, Count: 1}).Result()
		if err != nil {
			t.Fatal(err)
		}
		var received *redisDelivery
		next, err := backend.reclaimPage(ctx, transport.Subscription{Topology: topo}, "new", "0-0", func(delivery transport.Delivery) { received = delivery.(*redisDelivery) })
		if err != nil {
			t.Fatal(err)
		}
		if next != "0-0" || received == nil || received.xmsgID != d.xmsgID || received.DeliveryAttempt() != int(before[0].RetryCount)+1 || string(received.Message().Payload) != "original" {
			t.Fatal("claim lost identity/history")
		}
		if n, err := rc.XLen(ctx, d.stream).Result(); err != nil || n != 1 {
			t.Fatal("claim republished source")
		}
		if p, err := rc.XPending(ctx, d.stream, d.group).Result(); err != nil || p.Count != 1 {
			t.Fatal("claim ACKed unsettled source")
		}
		if want == 3 {
			if err := received.Settle(ctx, transport.Decision{Kind: transport.DecisionDeadLetter, Reason: "poison", Attempts: received.DeliveryAttempt()}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if p, err := rc.XPending(ctx, d.stream, d.group).Result(); err != nil || p.Count != 0 {
		t.Fatal("reclaimed source could not settle")
	}
}

func TestReclaimSkipsActiveAndMalformedRecordsRemainPending(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{JobType: "active"}
	d := pendingDelivery(t, backend, rc, topo)
	called := false
	if _, err := backend.reclaimPage(ctx, transport.Subscription{Topology: topo}, "new", "0-0", func(transport.Delivery) { called = true }); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("active entry reclaimed before idle threshold")
	}
	err := backend.deliverMessage(ctx, transport.Subscription{Topology: topo}, goredis.XMessage{ID: d.xmsgID, Values: map[string]any{}}, func(delivery transport.Delivery) {
		called = true
		if len(delivery.Message().Payload) != 0 {
			t.Fatal("malformed record not exposed as decode failure")
		}
	})
	if err != nil || !called {
		t.Fatal("malformed record silently dropped")
	}
	if p, err := rc.XPending(ctx, d.stream, d.group).Result(); err != nil || p.Count != 1 {
		t.Fatal("malformed record prematurely ACKed")
	}
}

func TestConsumeReclaimsBeforeFreshIntakeAndStopsOnCancellation(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	topo := transport.Topology{JobType: "consume_reclaim", Options: map[string]any{"redis.ack_wait": time.Millisecond}}
	d := pendingDelivery(t, backend, rc, topo)
	if err := rc.Do(ctx, "XCLAIM", d.stream, d.group, "old", 0, d.xmsgID, "IDLE", time.Hour.Milliseconds()).Err(); err != nil {
		t.Fatal(err)
	}
	var received *redisDelivery
	err := backend.Consume(ctx, transport.Subscription{Topology: topo}, func(delivery transport.Delivery) {
		received = delivery.(*redisDelivery)
		cancel()
	})
	if err != context.Canceled || received == nil || received.xmsgID != d.xmsgID || received.DeliveryAttempt() < 2 {
		t.Fatalf("consume reclaim = %v, %v", received, err)
	}
	if n, err := rc.XLen(context.Background(), d.stream).Result(); err != nil || n != 1 {
		t.Fatal("consume republished reclaimed entry")
	}
}

func TestConsumeAcquiresAtMostCombinedBudgetAcrossStreams(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	topo := transport.Topology{Namespace: "test", JobType: "combined-budget", Kind: transport.EventTopology, Options: map[string]any{"redis.block": time.Millisecond, redisEventLayoutOption: redisTargetedLayoutV1}}
	ensureTargetedEventTopology(t, backend, topo)
	sub := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	group := backend.groupName(sub)
	for _, stream := range []string{backend.streamKey(topo), backend.subscriberRetryStreamKey(topo, group)} {
		for index := range 3 {
			if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"payload": index}}).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	delivered := make(chan transport.Delivery, 10)
	done := make(chan error, 1)
	go func() { done <- backend.Consume(ctx, sub, func(delivery transport.Delivery) { delivered <- delivery }) }()
	for range 2 {
		select {
		case <-delivered:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for bounded intake")
		}
	}
	time.Sleep(20 * time.Millisecond)
	if got := len(delivered); got != 0 {
		t.Fatalf("delivered beyond combined budget: %d", got+2)
	}
	sharedPending, err := rc.XPending(ctx, backend.streamKey(topo), group).Result()
	if err != nil {
		t.Fatal(err)
	}
	privatePending, err := rc.XPending(ctx, backend.subscriberRetryStreamKey(topo, group), group).Result()
	if err != nil {
		t.Fatal(err)
	}
	if sharedPending.Count+privatePending.Count != 2 {
		t.Fatalf("broker acquired %d messages, want 2", sharedPending.Count+privatePending.Count)
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("consume shutdown = %v", err)
	}
}

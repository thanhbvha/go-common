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

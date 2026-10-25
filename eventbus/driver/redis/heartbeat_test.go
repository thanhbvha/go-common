package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestHeartbeatRenewsWithoutIncreasingHistory(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{JobType: "heartbeat", Options: map[string]any{"redis.ack_wait": time.Second}}
	d := pendingDelivery(t, backend, rc, topo)
	d.consumer = "test"
	if err := rc.Do(ctx, "XCLAIM", d.stream, d.group, d.consumer, 0, d.xmsgID, "IDLE", 5000, "JUSTID").Err(); err != nil {
		t.Fatal(err)
	}
	before, err := rc.XPendingExt(ctx, &goredis.XPendingExtArgs{Stream: d.stream, Group: d.group, Start: d.xmsgID, End: d.xmsgID, Count: 1}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := rc.XPendingExt(ctx, &goredis.XPendingExtArgs{Stream: d.stream, Group: d.group, Start: d.xmsgID, End: d.xmsgID, Count: 1}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if after[0].Idle >= time.Second || after[0].RetryCount != before[0].RetryCount {
		t.Fatalf("heartbeat did not reset idle or changed history: before=%+v after=%+v", before, after)
	}
	called := false
	if _, err := backend.reclaimPage(ctx, transport.Subscription{Topology: topo}, "competitor", "0-0", func(transport.Delivery) { called = true }); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("renewed active job reclaimed")
	}
	if d.HeartbeatInterval() != time.Second/3 {
		t.Fatal("heartbeat cadence ignores lease")
	}
}

func TestHeartbeatCannotStealOwnershipAndStaleSettlementFails(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{JobType: "lost"}
	d := pendingDelivery(t, backend, rc, topo)
	d.consumer = "test"
	if err := rc.XClaim(ctx, &goredis.XClaimArgs{Stream: d.stream, Group: d.group, Consumer: "new", MinIdle: 0, Messages: []string{d.xmsgID}}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := d.Heartbeat(ctx); !errors.Is(err, transport.ErrLeaseLost) {
		t.Fatalf("heartbeat = %v", err)
	}
	for _, kind := range []transport.DecisionKind{transport.DecisionAck, transport.DecisionRetry, transport.DecisionDeadLetter} {
		if err := d.Settle(ctx, transferDecision(kind)); !errors.Is(err, transport.ErrLeaseLost) {
			t.Fatalf("stale %s = %v", kind, err)
		}
	}
	if n, err := rc.Exists(ctx, backend.delayedKey(topo), backend.dlqKey(topo)).Result(); err != nil || n != 0 {
		t.Fatal("stale owner wrote destination")
	}
	pending, err := rc.XPendingExt(ctx, &goredis.XPendingExtArgs{Stream: d.stream, Group: d.group, Start: d.xmsgID, End: d.xmsgID, Count: 1}).Result()
	if err != nil || len(pending) != 1 || pending[0].Consumer != "new" {
		t.Fatal("stale heartbeat/settlement stole ownership")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := d.Heartbeat(canceled); err == nil {
		t.Fatal("heartbeat ignored cancellation")
	}
}

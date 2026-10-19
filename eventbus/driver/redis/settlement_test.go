package redis

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func pendingDelivery(t *testing.T, backend *redisTransport, rc *goredis.Client, topo transport.Topology) *redisDelivery {
	t.Helper()
	ctx := context.Background()
	stream := backend.streamKey(topo)
	if err := rc.XGroupCreateMkStream(ctx, stream, "workers", "0").Err(); err != nil {
		t.Fatal(err)
	}
	id, err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"payload": "original"}}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rc.XReadGroup(ctx, &goredis.XReadGroupArgs{Group: "workers", Consumer: "test", Streams: []string{stream, ">"}, Count: 1}).Result(); err != nil {
		t.Fatal(err)
	}
	return &redisDelivery{transport: backend, topo: topo, stream: stream, group: "workers", xmsgID: id, msg: transport.Message{Payload: []byte("original")}, count: 1}
}

func transferDecision(kind transport.DecisionKind) transport.Decision {
	return transport.Decision{Kind: kind, Next: transport.Message{Payload: []byte(`{"id":"retry","attempt":2}`)}, Source: &transport.Message{Payload: []byte(`{"id":"source","headers":{"correlation-id":"synthetic"}}`)}, RetryAt: time.Unix(1700000000, 0), Reason: "synthetic", Cause: errors.New("synthetic error"), Attempts: 2, FailedAt: time.Unix(1699999999, 0)}
}

func TestTransferCapacityFailurePreservesPendingSource(t *testing.T) {
	for _, kind := range []transport.DecisionKind{transport.DecisionRetry, transport.DecisionDeadLetter} {
		t.Run(kind.String(), func(t *testing.T) {
			backend, rc := delayedBackend(t)
			ctx := context.Background()
			topo := transport.Topology{Namespace: "capacity", JobType: kind.String(), Options: map[string]any{
				"redis.max_delayed_entries": int64(1), "redis.max_dlq_entries": int64(1),
			}}
			delivery := pendingDelivery(t, backend, rc, topo)
			if kind == transport.DecisionRetry {
				if err := rc.ZAdd(ctx, backend.delayedKey(topo), goredis.Z{Score: 1, Member: "existing"}).Err(); err != nil { t.Fatal(err) }
			} else {
				if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: backend.dlqKey(topo), Values: map[string]any{"payload": "existing"}}).Err(); err != nil { t.Fatal(err) }
			}
			err := delivery.Settle(ctx, transferDecision(kind))
			if !errors.Is(err, transport.ErrCapacityExceeded) { t.Fatalf("settle error=%v", err) }
			pending, pendingErr := rc.XPending(ctx, delivery.stream, delivery.group).Result()
			if pendingErr != nil || pending.Count != 1 { t.Fatalf("pending=%+v err=%v", pending, pendingErr) }
		})
	}
}

// DS-40/42/44: destination durability and source ACK are one isolated operation;
// failed writes retain pending ownership and the guard permits a later retry.
func TestAtomicTransferFailureAndRetry(t *testing.T) {
	for _, kind := range []transport.DecisionKind{transport.DecisionRetry, transport.DecisionDeadLetter} {
		t.Run(kind.String(), func(t *testing.T) {
			backend, rc := delayedBackend(t)
			ctx := context.Background()
			topo := transport.Topology{Namespace: "test", JobType: "atomic", Options: map[string]any{"redis.dlq_maxlen": int64(1)}}
			d := pendingDelivery(t, backend, rc, topo)
			decision := transferDecision(kind)
			target := backend.dlqKey(topo)
			if kind == transport.DecisionRetry {
				target = backend.delayedKey(topo)
			}
			if err := rc.Set(ctx, target, "wrong type", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := d.Settle(ctx, decision); err == nil {
				t.Fatal("invalid destination succeeded")
			}
			pending, err := rc.XPending(ctx, d.stream, d.group).Result()
			if err != nil || pending.Count != 1 {
				t.Fatal("source ACKed on failed destination write")
			}
			if err := rc.Del(ctx, target).Err(); err != nil {
				t.Fatal(err)
			}
			if kind == transport.DecisionDeadLetter {
				if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: target, Values: map[string]any{"payload": "existing"}}).Err(); err != nil {
					t.Fatal(err)
				}
			}
			if err := d.Settle(ctx, decision); err != nil {
				t.Fatal(err)
			}
			pending, err = rc.XPending(ctx, d.stream, d.group).Result()
			if err != nil || pending.Count != 0 {
				t.Fatal("successful transfer did not ACK source")
			}
			if kind == transport.DecisionRetry {
				values, err := rc.ZRangeWithScores(ctx, target, 0, -1).Result()
				if err != nil || len(values) != 1 || values[0].Member != string(decision.Next.Payload) || values[0].Score != float64(decision.RetryAt.UnixMilli()) {
					t.Fatal("retry metadata or deadline changed")
				}
			} else {
				values, err := rc.XRange(ctx, target, "-", "+").Result()
				if err != nil || len(values) != 2 {
					t.Fatal("DLQ transfer trimmed existing record")
				}
				v := values[1].Values
				if v["payload"] != string(decision.Source.Payload) || v["source_id"] != d.xmsgID || v["source_stream"] != d.stream || v["source_group"] != d.group || v["reason"] != "synthetic" || v["error"] != "synthetic error" || v["attempts"] != "2" || v["failed_at"] != "1699999999000" {
					t.Fatalf("DLQ metadata incorrect: %v", v)
				}
			}
			// A fresh delivery models another instance / ambiguous response retry.
			stale := &redisDelivery{transport: backend, topo: topo, stream: d.stream, group: d.group, xmsgID: d.xmsgID, msg: d.msg}
			if err := stale.Settle(ctx, decision); !errors.Is(err, transport.ErrDeliverySettled) {
				t.Fatalf("stale transfer = %v", err)
			}
		})
	}
}

func TestAtomicTransferMissingGroupWritesNothing(t *testing.T) {
	for _, kind := range []transport.DecisionKind{transport.DecisionRetry, transport.DecisionDeadLetter} {
		t.Run(kind.String(), func(t *testing.T) {
			backend, rc := delayedBackend(t)
			ctx := context.Background()
			topo := transport.Topology{JobType: "missing"}
			d := pendingDelivery(t, backend, rc, topo)
			if err := rc.XGroupDestroy(ctx, d.stream, d.group).Err(); err != nil {
				t.Fatal(err)
			}
			if err := d.Settle(ctx, transferDecision(kind)); err == nil {
				t.Fatal("missing group accepted")
			}
			if count, err := rc.Exists(ctx, backend.delayedKey(topo), backend.dlqKey(topo)).Result(); err != nil || count != 0 {
				t.Fatal("destination written before source validation")
			}
		})
	}
}

func TestAtomicTransferConcurrentInstances(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{JobType: "concurrent"}
	d := pendingDelivery(t, backend, rc, topo)
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			copy := &redisDelivery{transport: backend, topo: topo, stream: d.stream, group: d.group, xmsgID: d.xmsgID, msg: d.msg}
			results <- copy.Settle(ctx, transferDecision(transport.DecisionDeadLetter))
		}()
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, transport.ErrDeliverySettled) {
			t.Fatal(err)
		}
	}
	if success != 1 {
		t.Fatalf("successful transfers = %d", success)
	}
	if count, err := rc.XLen(ctx, backend.dlqKey(topo)).Result(); err != nil || count != 1 {
		t.Fatal("concurrent DLQ duplication")
	}
}

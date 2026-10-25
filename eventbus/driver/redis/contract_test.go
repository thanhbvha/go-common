package redis

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	buscontract "github.com/thanhbvha/go-common/eventbus/contracttest"
	"github.com/thanhbvha/go-common/eventbus/transport"
	transportcontract "github.com/thanhbvha/go-common/eventbus/transport/contracttest"
)

func TestRedisTransportContract(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	var ambiguityClients sync.Map
	transportcontract.Run(t, transportcontract.Harness{
		New: func(t *testing.T) transport.Transport {
			backend := New(rc)
			t.Cleanup(func() { _ = backend.Close(context.Background()) })
			return backend
		},
		Topology: func(name string, kind transport.TopologyKind) transport.Topology {
			topo := transport.Topology{
				Namespace: "contract", JobType: name, Kind: kind,
				Options: map[string]any{"redis.block": 10 * time.Millisecond, "redis.ack_wait": time.Millisecond},
			}
			if kind == transport.EventTopology {
				topo.Options[redisEventLayoutOption] = redisTargetedLayoutV1
			}
			return topo
		},
		DispatchDue: func(ctx context.Context, tr transport.Transport, topo transport.Topology, now time.Time) (int64, error) {
			return tr.(*redisTransport).dispatchDue(ctx, topo, now)
		},
		ExpireDedupe:               func(_ *testing.T, duration time.Duration) { mr.FastForward(duration) },
		PrepareUnsettledRedelivery: func(*testing.T, transport.Transport, transport.Topology) { mr.FastForward(2 * time.Millisecond) },
		PostCommitAmbiguity: func(t *testing.T, tr transport.Transport, topo transport.Topology, kind transport.DecisionKind, phase transportcontract.AmbiguityPhase) transportcontract.AmbiguityState {
			backend := tr.(*redisTransport)
			switch phase {
			case transportcontract.AmbiguityArm:
				faultClient := goredis.NewClient(rc.Options())
				if err := transferScript.Load(context.Background(), faultClient).Err(); err != nil {
					t.Fatal(err)
				}
				faultClient.AddHook(&postSuccessErrorHook{
					commands: map[string]bool{"evalsha": true}, scriptHashes: map[string]bool{transferScript.Hash(): true},
				})
				ambiguityClients.Store(backend, faultClient)
				backend.rc = faultClient
				return transportcontract.AmbiguityState{}
			case transportcontract.AmbiguityObserve, transportcontract.AmbiguityFinal:
				if value, ok := ambiguityClients.LoadAndDelete(backend); ok {
					backend.rc = rc
					_ = value.(*goredis.Client).Close()
				}
				stats, err := backend.Stats(context.Background(), topo)
				if err != nil && !errors.Is(err, transport.ErrNotSupported) {
					t.Fatal(err)
				}
				count := stats.DLQ
				if kind == transport.DecisionRetry {
					count = stats.Delayed
				}
				return transportcontract.AmbiguityState{SourceOutstanding: stats.InFlight > 0, DestinationCount: count, ExpectedFinalDestinations: 1}
			default:
				t.Fatalf("unknown ambiguity phase %d", phase)
				return transportcontract.AmbiguityState{}
			}
		},
		SchedulerOwnershipRecovery: func(t *testing.T, first, second transport.Transport, topo transport.Topology, now time.Time) int {
			firstBackend := first.(*redisTransport)
			secondBackend := second.(*redisTransport)
			ctx := context.Background()
			if err := rc.Set(ctx, firstBackend.streamKey(topo), "wrong type", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if _, err := firstBackend.dispatchDue(ctx, topo, now); err == nil {
				t.Fatal("failed Redis scheduler owner unexpectedly dispatched")
			}
			if delayed, err := rc.ZCard(ctx, firstBackend.delayedKey(topo)).Result(); err != nil || delayed != 1 {
				t.Fatalf("delayed source after owner failure = %d, %v", delayed, err)
			}
			if err := rc.Del(ctx, firstBackend.streamKey(topo)).Err(); err != nil {
				t.Fatal(err)
			}
			if moved, err := secondBackend.dispatchDue(ctx, topo, now); err != nil || moved != 1 {
				t.Fatalf("successor Redis dispatch = %d, %v", moved, err)
			}
			return 1
		},
		SubscriberRetryIsolation: true,
		SubscriberDLQIsolation:   true,
	})
}

func TestRedisBusContract(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	var backend *redisTransport
	buscontract.Run(t, buscontract.Harness{
		New: func(t *testing.T) transport.Transport {
			created := New(rc).(*redisTransport)
			backend = created
			t.Cleanup(func() { _ = created.Close(context.Background()) })
			return created
		},
		Topology: func(name string, kind transport.TopologyKind) transport.Topology {
			return transport.Topology{Namespace: "redis-bus-contract", JobType: name, Kind: kind, Options: map[string]any{
				"redis.block": 10 * time.Millisecond, "redis.ack_wait": time.Minute,
			}}
		},
		DispatchDue: func(ctx context.Context, tr transport.Transport, topo transport.Topology, now time.Time) (int64, error) {
			return tr.(*redisTransport).dispatchDue(ctx, topo, now)
		},
		PrepareUnsettledRedelivery: func(t *testing.T, topo transport.Topology) {
			pending, err := rc.XPendingExt(context.Background(), &goredis.XPendingExtArgs{
				Stream: backend.streamKey(topo), Group: backend.groupName(transport.Subscription{Topology: topo}),
				Start: "-", End: "+", Count: 10,
			}).Result()
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range pending {
				if err := rc.Do(context.Background(), "XCLAIM", backend.streamKey(topo), backend.groupName(transport.Subscription{Topology: topo}), "contract-stale", 0, entry.ID, "IDLE", (2 * time.Minute).Milliseconds()).Err(); err != nil {
					t.Fatal(err)
				}
			}
		},
		DurableRedelivery:        true,
		RepeatedReconnect:        true,
		ReconnectSessionLifetime: 25 * time.Millisecond,
	})
}

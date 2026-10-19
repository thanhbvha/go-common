package redis

import (
	"context"
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
		PrepareUnsettledRedelivery: func(_ *testing.T, _ transport.Topology) { mr.FastForward(2 * time.Millisecond) },
		SubscriberRetryIsolation:   true,
		SubscriberDLQIsolation:     true,
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
		DurableRedelivery: true,
	})
}

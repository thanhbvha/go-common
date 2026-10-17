package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/thanhbvha/go-common/eventbus/transport/contracttest"
)

func TestRedisTransportContract(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	contracttest.Run(t, contracttest.Harness{
		New: func(t *testing.T) transport.Transport {
			backend := New(rc)
			t.Cleanup(func() { _ = backend.Close(context.Background()) })
			return backend
		},
		Topology: func(name string, kind transport.TopologyKind) transport.Topology {
			return transport.Topology{
				Namespace: "contract", JobType: name, Kind: kind,
				Options: map[string]any{"redis.block": 10 * time.Millisecond, "redis.ack_wait": time.Minute},
			}
		},
		DispatchDue: func(ctx context.Context, tr transport.Transport, topo transport.Topology, now time.Time) (int64, error) {
			return tr.(*redisTransport).dispatchDue(ctx, topo, now)
		},
		ExpireDedupe: func(_ *testing.T, duration time.Duration) { mr.FastForward(duration) },
	})
}

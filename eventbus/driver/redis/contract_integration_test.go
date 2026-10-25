//go:build integration

package redis

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/thanhbvha/go-common/eventbus/transport/contracttest"
)

func TestRedisSingleNodeContractIntegration(t *testing.T) {
	addr := os.Getenv("EVENTBUS_REDIS_ADDR")
	if addr == "" {
		t.Skip("set EVENTBUS_REDIS_ADDR to run the single-node Redis contract")
	}
	rc := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = rc.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rc.Ping(ctx).Err(); err != nil {
		t.Skipf("single-node Redis unavailable at %s: %v", addr, err)
	}
	runLiveRedisContract(t, rc, "single")
}

func TestRedisClusterContractIntegration(t *testing.T) {
	raw := os.Getenv("EVENTBUS_REDIS_CLUSTER_ADDRS")
	if raw == "" {
		t.Skip("set EVENTBUS_REDIS_CLUSTER_ADDRS to run the Redis Cluster contract")
	}
	addrs := strings.Split(raw, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	rc := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addrs})
	t.Cleanup(func() { _ = rc.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rc.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis Cluster unavailable at %v: %v", addrs, err)
	}
	info, err := rc.ClusterInfo(ctx).Result()
	if err != nil || !strings.Contains(info, "cluster_state:ok") {
		t.Fatalf("Redis Cluster is not healthy: %q, %v", info, err)
	}
	runLiveRedisContract(t, rc, "cluster")
}

func runLiveRedisContract(t *testing.T, rc goredis.UniversalClient, mode string) {
	t.Helper()
	namespace := fmt.Sprintf("p38-%s-%d", mode, time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := deleteNamespaceKeys(ctx, rc, namespace); err != nil {
			t.Errorf("clean Redis integration keys: %v", err)
		}
	})
	contracttest.Run(t, contracttest.Harness{
		New: func(t *testing.T) transport.Transport {
			backend := New(rc)
			t.Cleanup(func() { _ = backend.Close(context.Background()) })
			return backend
		},
		Topology: func(name string, kind transport.TopologyKind) transport.Topology {
			topo := transport.Topology{
				Namespace: namespace, JobType: name, Kind: kind,
				Options: map[string]any{"redis.block": 25 * time.Millisecond, "redis.ack_wait": time.Minute},
			}
			if kind == transport.EventTopology {
				topo.Options[redisEventLayoutOption] = redisTargetedLayoutV1
			}
			return topo
		},
		DispatchDue: func(ctx context.Context, tr transport.Transport, topo transport.Topology, now time.Time) (int64, error) {
			return tr.(*redisTransport).dispatchDue(ctx, topo, now)
		},
		ExpireDedupe: func(t *testing.T, ttl time.Duration) {
			t.Helper()
			time.Sleep(ttl + 50*time.Millisecond)
		},
		PrepareUnsettledRedelivery: func(t *testing.T, tr transport.Transport, topo transport.Topology) {
			t.Helper()
			backend := tr.(*redisTransport)
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
		SubscriberRetryIsolation: true,
		SubscriberDLQIsolation:   true,
		ReceiveWithin:            10 * time.Second,
	})
}

func deleteNamespaceKeys(ctx context.Context, rc goredis.UniversalClient, namespace string) error {
	prefix := "eventbus:v1:{" + base64.RawURLEncoding.EncodeToString([]byte(namespace)) + ":"
	deleteFrom := func(ctx context.Context, client *goredis.Client) error {
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
			if err != nil {
				return err
			}
			for _, key := range keys {
				if err := client.Del(ctx, key).Err(); err != nil {
					return err
				}
			}
			cursor = next
			if cursor == 0 {
				return nil
			}
		}
	}
	switch client := rc.(type) {
	case *goredis.Client:
		return deleteFrom(ctx, client)
	case *goredis.ClusterClient:
		return client.ForEachMaster(ctx, deleteFrom)
	default:
		return fmt.Errorf("unsupported integration client %T", rc)
	}
}

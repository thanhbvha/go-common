//go:build integration

package redis

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/v2/eventbus/transport"
)

func TestRedisSingleNodeIntakeWakeupIntegration(t *testing.T) {
	addr := os.Getenv("EVENTBUS_REDIS_ADDR")
	if addr == "" {
		t.Skip("set EVENTBUS_REDIS_ADDR")
	}
	rc := goredis.NewClient(&goredis.Options{Addr: addr})
	defer rc.Close()
	runLiveIntakeWakeup(t, rc)
}
func TestRedisClusterIntakeWakeupIntegration(t *testing.T) {
	raw := os.Getenv("EVENTBUS_REDIS_CLUSTER_ADDRS")
	if raw == "" {
		t.Skip("set EVENTBUS_REDIS_CLUSTER_ADDRS")
	}
	rc := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: strings.Split(raw, ",")})
	defer rc.Close()
	runLiveIntakeWakeup(t, rc)
}
func runLiveIntakeWakeup(t *testing.T, rc goredis.UniversalClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	waiting := make(chan struct{}, 1)
	rc.AddHook(intakeCommandHook{before: func(_ context.Context, cmd goredis.Cmder) error {
		if cmd.Name() == "xread" {
			waiting <- struct{}{}
		}
		return nil
	}})
	if err := rc.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	namespace := fmt.Sprintf("intake-%d", time.Now().UnixNano())
	defer func() {
		cleanup, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		if err := deleteNamespaceKeys(cleanup, rc, namespace); err != nil {
			t.Error(err)
		}
	}()
	backend := New(rc).(*redisTransport)
	defer backend.Close(context.Background())
	topo := targetedEventTopology(namespace, "wake")
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Group: "router", Concurrency: 1}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	r := streamIntake{client: rc, streams: []string{backend.streamKey(topo), backend.subscriberRetryStreamKey(topo, backend.groupName(sub))}, group: backend.groupName(sub), consumer: "live", block: time.Second}
	// Hook signals each read-only waiter. The test wakes it from each lane in
	// turn; a one-permit budget may never create pending entries in both lanes.

	for _, lane := range r.streams {
		var wg sync.WaitGroup
		done := make(chan error, 1)
		wg.Go(func() { _, err := r.read(ctx, 1); done <- err })
		select {
		case <-waiting:
		case err := <-done:
			t.Fatalf("intake returned before waiting on lane %s: %v", lane, err)
		case <-ctx.Done():
			t.Fatalf("waiting for wake-up command on lane %s: %v", lane, ctx.Err())
		}
		if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: lane, Values: map[string]any{"payload": "live"}}).Err(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != goredis.Nil {
				t.Fatalf("wake: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for append wake-up on lane %s: %v", lane, ctx.Err())
		}
		wg.Wait()
		for _, stream := range r.streams {
			p, err := rc.XPending(ctx, stream, r.group).Result()
			if err != nil || p.Count != 0 {
				t.Fatalf("read-only wake acquired pending: %v %v", p, err)
			}
		}
		got, err := r.read(ctx, 1)
		if err != nil || len(got) != 1 || got[0].Stream != lane || len(got[0].Messages) != 1 {
			t.Fatalf("intake: %v %v", got, err)
		}
		if err := rc.XAck(ctx, lane, r.group, got[0].Messages[0].ID).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

//go:build integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestRedisSingleNodeRetentionIntegration(t *testing.T) {
	addr := os.Getenv("EVENTBUS_REDIS_ADDR")
	if addr == "" {
		t.Skip("set EVENTBUS_REDIS_ADDR to run live Redis retention tests")
	}
	rc := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = rc.Close() })
	runLiveRetentionContract(t, rc, "single")
}

func TestRedisClusterRetentionIntegration(t *testing.T) {
	raw := os.Getenv("EVENTBUS_REDIS_CLUSTER_ADDRS")
	if raw == "" {
		t.Skip("set EVENTBUS_REDIS_CLUSTER_ADDRS to run live Redis Cluster retention tests")
	}
	addrs := strings.Split(raw, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	rc := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addrs})
	t.Cleanup(func() { _ = rc.Close() })
	runLiveRetentionContract(t, rc, "cluster")
}

func runLiveRetentionContract(t *testing.T, rc goredis.UniversalClient, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := rc.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis %s unavailable: %v", mode, err)
	}
	namespace := fmt.Sprintf("p671r-%s-%d", mode, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := deleteNamespaceKeys(cleanupCtx, rc, namespace); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})

	first := New(rc).(*redisTransport)
	t.Cleanup(func() { _ = first.Close(context.Background()) })
	topo := transport.Topology{Namespace: namespace, JobType: "watermark", Kind: transport.EventTopology,
		Options: map[string]any{"redis.retention_interval": time.Hour, "redis.retention_batch_size": int64(100)}}
	for _, group := range []string{"fast", "slow"} {
		if err := first.EnsureSubscription(ctx, transport.Subscription{Topology: topo, Group: group}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 500 {
		if err := first.Publish(ctx, topo, transport.Message{Payload: []byte(fmt.Sprintf("message-%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	stream := first.streamKey(topo)
	readAck := func(group string, count int64, ack bool) []goredis.XMessage {
		t.Helper()
		result, err := rc.XReadGroup(ctx, &goredis.XReadGroupArgs{Group: "subscriber:" + group, Consumer: mode, Streams: []string{stream, ">"}, Count: count}).Result()
		if err != nil {
			t.Fatalf("read %s: %v", group, err)
		}
		messages := result[0].Messages
		if ack {
			ids := make([]string, len(messages))
			for i := range messages {
				ids[i] = messages[i].ID
			}
			if err := rc.XAck(ctx, stream, "subscriber:"+group, ids...).Err(); err != nil {
				t.Fatal(err)
			}
		}
		return messages
	}
	readAck("fast", 500, true)
	if _, err := first.compactReadyStream(ctx, topo); err != nil {
		t.Fatal(err)
	}
	if length, _ := rc.XLen(ctx, stream).Result(); length != 500 {
		t.Fatalf("offline group lost unread entries: %d", length)
	}
	slowPending := readAck("slow", 250, false)
	if _, err := first.compactReadyStream(ctx, topo); err != nil {
		t.Fatal(err)
	}
	if length, _ := rc.XLen(ctx, stream).Result(); length != 500 {
		t.Fatalf("pending entries were trimmed: %d", length)
	}
	ids := make([]string, len(slowPending))
	for i := range slowPending {
		ids[i] = slowPending[i].ID
	}
	if err := rc.XAck(ctx, stream, "subscriber:slow", ids...).Err(); err != nil {
		t.Fatal(err)
	}
	readAck("slow", 500, true)

	// Two instances race on the optimization lease. Epoch fencing and the
	// conservative watermark make either execution order loss-safe.
	second := New(rc).(*redisTransport)
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	var wg sync.WaitGroup
	for _, backend := range []*redisTransport{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 16 {
				_, _ = backend.compactReadyStream(ctx, topo)
			}
		}()
	}
	wg.Wait()
	var remaining int64
	for range 32 {
		var err error
		remaining, err = rc.XLen(ctx, stream).Result()
		if err != nil {
			t.Fatal(err)
		}
		if remaining <= 1 {
			break
		}
		_, _ = first.compactReadyStream(ctx, topo)
		_, _ = second.compactReadyStream(ctx, topo)
	}
	if remaining > 1 {
		t.Fatalf("acknowledged backlog not reclaimed: remaining=%d first=%+v second=%+v", remaining, first.RetentionStatus(topo), second.RetentionStatus(topo))
	}

	// Manifest obligations survive transport restart.
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted := New(rc).(*redisTransport)
	t.Cleanup(func() { _ = restarted.Close(context.Background()) })
	report, err := restarted.RetentionReport(ctx, topo)
	if err != nil || report.ActiveGroups != 2 || report.MissingGroups != 0 || report.UnmanagedGroups != 0 {
		t.Fatalf("restart report=%+v err=%v", report, err)
	}

	capacityTopo := transport.Topology{Namespace: namespace, JobType: "offline-capacity", Kind: transport.EventTopology,
		Options: map[string]any{"redis.max_ready_entries": int64(32), "redis.retention_interval": time.Hour}}
	if err := restarted.EnsureSubscription(ctx, transport.Subscription{Topology: capacityTopo, Group: "offline"}); err != nil {
		t.Fatal(err)
	}
	for range 32 {
		if err := restarted.Publish(ctx, capacityTopo, transport.Message{Payload: []byte("bounded")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := restarted.Publish(ctx, capacityTopo, transport.Message{Payload: []byte("rejected")}); !errors.Is(err, transport.ErrCapacityExceeded) {
		t.Fatalf("capacity error=%v", err)
	}
	if length, _ := rc.XLen(ctx, restarted.streamKey(capacityTopo)).Result(); length != 32 {
		t.Fatalf("capacity length=%d", length)
	}

	// A stale candidate can never cross a concurrent manifest epoch change.
	oldEpoch, _ := rc.HGet(ctx, restarted.metadataKey(topo), "retention_epoch").Result()
	if err := restarted.EnsureSubscription(ctx, transport.Subscription{Topology: topo, Group: "new"}); err != nil {
		t.Fatal(err)
	}
	stale, err := trimAtEpochScript.Run(ctx, rc, []string{
		stream, restarted.metadataKey(topo), restarted.groupManifestKey(topo),
	}, oldEpoch, "+", 100).Int64()
	if err != nil || stale != -1 {
		t.Fatalf("stale epoch result=%d err=%v", stale, err)
	}
}

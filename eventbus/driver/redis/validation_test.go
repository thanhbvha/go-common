package redis

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/v2/eventbus/transport"
)

func targetedEventTopology(namespace, name string) transport.Topology {
	return transport.Topology{Namespace: namespace, JobType: name, Kind: transport.EventTopology,
		Options: map[string]any{redisEventLayoutOption: redisTargetedLayoutV1}}
}

func ensureTargetedEventTopology(t *testing.T, backend *redisTransport, topo transport.Topology) {
	t.Helper()
	backend.mu.Lock()
	closed := backend.closed
	backend.mu.Unlock()
	if closed {
		if err := backend.rc.HSet(context.Background(), backend.metadataKey(topo), map[string]any{
			"layout_version": "1", "namespace": topo.Namespace, "kind": topo.Kind.String(),
			"name": topo.JobType, "event_layout": redisTargetedLayoutV1,
		}).Err(); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
}

func TestValidateTopologyRejectsInvalidRedisOptions(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value any
	}{
		{name: "maxlen zero", key: "redis.maxlen", value: int64(0)},
		{name: "batch wrong type", key: "redis.batch_size", value: 1},
		{name: "block negative", key: "redis.block", value: -time.Second},
		{name: "ack wait zero", key: "redis.ack_wait", value: time.Duration(0)},
		{name: "delay batch zero", key: "redis.delay_batch_size", value: 0},
		{name: "delay batch unbounded", key: "redis.delay_batch_size", value: 1001},
		{name: "unknown", key: "redis.unknown", value: 1},
	}

	validator := &redisTransport{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topology := transport.Topology{JobType: "task", Options: map[string]any{tt.key: tt.value}}
			if err := validator.ValidateTopology(topology); err == nil {
				t.Fatal("ValidateTopology unexpectedly succeeded")
			}
		})
	}
}

func TestConsumerBatchSizeIsBoundedByCoreCapacity(t *testing.T) {
	sub := transport.Subscription{
		Topology:    transport.Topology{Options: map[string]any{"redis.batch_size": int64(100)}},
		Concurrency: 3, Prefetch: 5,
	}
	if got := consumerBatchSize(sub); got != 8 {
		t.Fatalf("consumer batch size = %d, want 8", got)
	}
}

func TestValidateTopologyRequiresTargetedEventLayoutOnlyForEvents(t *testing.T) {
	validator := &redisTransport{}
	if err := validator.ValidateTopology(transport.Topology{JobType: "event", Kind: transport.EventTopology}); err == nil || !strings.Contains(err.Error(), "WithEventLayoutTargeted") {
		t.Fatalf("event without fence error = %v", err)
	}
	if err := validator.ValidateTopology(targetedEventTopology("", "event")); err != nil {
		t.Fatalf("targeted event rejected: %v", err)
	}
	if err := validator.ValidateTopology(transport.Topology{JobType: "task"}); err != nil {
		t.Fatalf("task changed: %v", err)
	}
	if err := validator.ValidateTopology(transport.Topology{JobType: "task", Options: map[string]any{redisEventLayoutOption: redisTargetedLayoutV1}}); err == nil {
		t.Fatal("task accepted event layout option")
	}
}

func TestEventLayoutMarkerFailsClosedOutsideEnsureTopology(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	backend := New(rc).(*redisTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	ctx := context.Background()
	topo := targetedEventTopology("migration", "fence")

	if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("blocked")}); err == nil || !strings.Contains(err.Error(), "marker is missing") {
		t.Fatalf("publish without marker error = %v", err)
	}
	if err := backend.EnsureSubscription(ctx, transport.Subscription{Topology: topo, Group: "subscriber"}); err == nil || !strings.Contains(err.Error(), "marker is missing") {
		t.Fatalf("subscription without marker error = %v", err)
	}
	if _, err := backend.Stats(ctx, topo); err == nil || !strings.Contains(err.Error(), "marker is missing") {
		t.Fatalf("admin without marker error = %v", err)
	}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("accepted")}); err != nil {
		t.Fatal(err)
	}
	if err := rc.HSet(ctx, backend.metadataKey(topo), "event_layout", "broadcast-v0").Err(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("blocked")}); err == nil || !strings.Contains(err.Error(), "marker mismatch") {
		t.Fatalf("publish with mismatched marker error = %v", err)
	}
	if err := backend.EnsureTopology(ctx, topo); err == nil || !strings.Contains(err.Error(), "marker mismatch") {
		t.Fatalf("EnsureTopology overwrote mismatched marker: %v", err)
	}
}

func TestFreshEventProvisioningAndRestartAreIdempotent(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	ctx := context.Background()
	topo := targetedEventTopology("fresh-fixture", "retention")
	topo.Options["eventbus.delay_disabled"] = true
	topo.Options["redis.retention_interval"] = time.Hour
	first := New(client).(*redisTransport)
	t.Cleanup(func() { first.Close(context.Background()) })
	sub := transport.Subscription{Topology: topo, Group: "offline"}
	if err := first.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	if err := first.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	if err := first.Publish(ctx, topo, transport.Message{Payload: []byte("retain unread")}); err != nil {
		t.Fatal(err)
	}
	epoch, err := client.HGet(ctx, first.metadataKey(topo), "retention_epoch").Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := New(client).(*redisTransport)
	t.Cleanup(func() { restarted.Close(context.Background()) })
	for range 2 {
		if err := restarted.EnsureTopology(ctx, topo); err != nil {
			t.Fatal(err)
		}
		if err := restarted.EnsureSubscription(ctx, sub); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := client.HGet(ctx, restarted.metadataKey(topo), "event_layout").Result(); err != nil || got != redisTargetedLayoutV1 {
		t.Fatal("layout marker missing", got, err)
	}
	if got, err := client.HGet(ctx, restarted.metadataKey(topo), "retention_epoch").Result(); err != nil || got != epoch {
		t.Fatal("reprovision changed manifest epoch", got, err)
	}
	report, err := restarted.RetentionReport(ctx, topo)
	if err != nil || report.ActiveGroups != 1 || report.MissingGroups != 0 || report.UnmanagedGroups != 0 || report.ReadyEntries != 1 {
		t.Fatalf("reprovision lost retention state: %+v, %v", report, err)
	}
}

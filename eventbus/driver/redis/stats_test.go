package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func addStatsMessages(t *testing.T, backend *redisTransport, rc *goredis.Client, topo transport.Topology, count int, oldest time.Time) []string {
	t.Helper()
	ids := make([]string, 0, count)
	for i := range count {
		id := fmt.Sprintf("%d-0", oldest.Add(time.Duration(i)*time.Millisecond).UnixMilli())
		if err := rc.XAdd(context.Background(), &goredis.XAddArgs{
			Stream: backend.streamKey(topo), ID: id,
			Values: map[string]any{"payload": "{}"},
		}).Err(); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func createStatsGroup(t *testing.T, backend *redisTransport, rc *goredis.Client, topo transport.Topology, group string) {
	t.Helper()
	ensureTargetedEventTopology(t, backend, topo)
	if err := rc.XGroupCreateMkStream(context.Background(), backend.streamKey(topo), group, "0").Err(); err != nil {
		t.Fatal(err)
	}
}

func readStatsGroup(t *testing.T, backend *redisTransport, rc *goredis.Client, topo transport.Topology, group string, count int64) {
	t.Helper()
	if _, err := rc.XReadGroup(context.Background(), &goredis.XReadGroupArgs{
		Group: group, Consumer: "consumer", Streams: []string{backend.streamKey(topo), ">"}, Count: count,
	}).Result(); err != nil {
		t.Fatal(err)
	}
}

func TestRedisStatsTaskReportsRealBacklog(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "test", JobType: "stats-task", Kind: transport.TaskTopology}
	createStatsGroup(t, backend, rc, topo, "workers")
	addStatsMessages(t, backend, rc, topo, 4, time.Now().Add(-10*time.Second))
	readStatsGroup(t, backend, rc, topo, "workers", 2)
	for i := range 3 {
		if err := rc.ZAdd(ctx, backend.delayedKey(topo), goredis.Z{Score: float64(i), Member: fmt.Sprintf("delayed-%d", i)}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rc.HSet(ctx, backend.metadataKey(topo), "delayed_entries", 3).Err(); err != nil {
		t.Fatal(err)
	}
	addDLQRecord(t, backend, rc, topo, "1-0", 1)

	stats, err := backend.Stats(ctx, topo)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 2 || stats.InFlight != 2 || stats.Delayed != 3 || stats.DLQ != 1 || stats.Lag != 4 || !stats.LagKnown {
		t.Fatalf("stats = %#v", stats)
	}
	if !stats.OldestAgeKnown || stats.OldestAge < 9*time.Second || stats.OldestAge > 12*time.Second {
		t.Fatalf("oldest age = %v, known = %v", stats.OldestAge, stats.OldestAgeKnown)
	}
}

func TestRedisStatsProducerOnlyTaskTreatsStreamAsReady(t *testing.T) {
	backend, rc := delayedBackend(t)
	topo := transport.Topology{Namespace: "test", JobType: "producer-only", Kind: transport.TaskTopology}
	addStatsMessages(t, backend, rc, topo, 2, time.Now().Add(-time.Second))

	stats, err := backend.Stats(context.Background(), topo)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 2 || stats.InFlight != 0 || stats.Lag != 2 || !stats.LagKnown || !stats.OldestAgeKnown {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestRedisStatsUntouchedGroupCountsReadyWithoutLagMetadata(t *testing.T) {
	backend, rc := delayedBackend(t)
	topo := transport.Topology{Namespace: "test", JobType: "untouched-group", Kind: transport.TaskTopology}
	createStatsGroup(t, backend, rc, topo, "workers")
	addStatsMessages(t, backend, rc, topo, 3, time.Now().Add(-time.Second))

	stats, err := backend.Stats(context.Background(), topo)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 3 || stats.InFlight != 0 || stats.Lag != 3 || !stats.LagKnown || !stats.OldestAgeKnown {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestRedisStatsAvailableThroughBus(t *testing.T) {
	backend, _ := delayedBackend(t)
	instance, err := eventbus.Open(eventbus.DefaultConfig(), eventbus.WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	if err := instance.DeclareTask("bus-stats"); err != nil {
		t.Fatal(err)
	}
	if err := instance.Enqueue(context.Background(), "bus-stats", "payload"); err != nil {
		t.Fatal(err)
	}
	stats, err := instance.TopologyStats(context.Background(), eventbus.TaskRef("bus-stats"))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 1 || stats.Lag != 1 || !stats.LagKnown || !stats.OldestAgeKnown {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestRedisStatsFallbackPagination(t *testing.T) {
	backend, rc := delayedBackend(t)
	topo := transport.Topology{Namespace: "test", JobType: "stats-pages"}
	createStatsGroup(t, backend, rc, topo, "workers")
	addStatsMessages(t, backend, rc, topo, 1001, time.Now().Add(-time.Second))
	readStatsGroup(t, backend, rc, topo, "workers", 1)

	stats, err := backend.Stats(context.Background(), topo)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 1000 || stats.InFlight != 1 || stats.Lag != 1001 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestRedisStatsEventAggregatesSubscriberObligations(t *testing.T) {
	backend, rc := delayedBackend(t)
	topo := targetedEventTopology("test", "stats-event")
	createStatsGroup(t, backend, rc, topo, "subscriber:email")
	createStatsGroup(t, backend, rc, topo, "subscriber:audit")
	createStatsGroup(t, backend, rc, topo, "foreign")
	addStatsMessages(t, backend, rc, topo, 3, time.Now().Add(-time.Second))
	readStatsGroup(t, backend, rc, topo, "subscriber:email", 1)
	readStatsGroup(t, backend, rc, topo, "subscriber:audit", 2)

	stats, err := backend.Stats(context.Background(), topo)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 3 || stats.InFlight != 3 || stats.Lag != 6 || !stats.LagKnown || !stats.OldestAgeKnown {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestRedisStatsIncludeSubscriberPrivateLanes(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := targetedEventTopology("test", "stats-private")
	ensureTargetedEventTopology(t, backend, topo)
	alpha := transport.Subscription{Topology: topo, Group: "alpha"}
	beta := transport.Subscription{Topology: topo, Group: "beta"}
	for _, sub := range []transport.Subscription{alpha, beta} {
		if err := backend.EnsureSubscription(ctx, sub); err != nil {
			t.Fatal(err)
		}
	}
	alphaGroup := backend.groupName(alpha)
	if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: backend.subscriberRetryStreamKey(topo, alphaGroup), Values: map[string]any{"payload": "ready"}}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rc.ZAdd(ctx, backend.subscriberRetryDelayedKey(topo, alphaGroup), goredis.Z{Score: 1, Member: "delayed"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rc.HSet(ctx, backend.metadataKey(topo), "delayed_entries", 1).Err(); err != nil {
		t.Fatal(err)
	}
	stats, err := backend.Stats(ctx, topo)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 1 || stats.InFlight != 0 || stats.Lag != 1 || stats.Delayed != 1 {
		t.Fatalf("topology stats = %+v", stats)
	}
	alphaStats, err := backend.SubscriptionStats(ctx, alpha)
	if err != nil || alphaStats.Pending != 1 || alphaStats.Delayed != 1 || alphaStats.Lag != 1 {
		t.Fatalf("alpha stats = %+v, %v", alphaStats, err)
	}
	betaStats, err := backend.SubscriptionStats(ctx, beta)
	if err != nil || betaStats.Pending != 0 || betaStats.Delayed != 0 || betaStats.Lag != 0 {
		t.Fatalf("beta stats = %+v, %v", betaStats, err)
	}
}

func TestRedisStatsEmptyTopologyIsKnownZero(t *testing.T) {
	backend, _ := delayedBackend(t)
	topo := transport.Topology{Namespace: "test", JobType: "empty"}
	stats, err := backend.Stats(context.Background(), topo)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (transport.TopologyStats{LagKnown: true, OldestAgeKnown: true}) {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestRedisStatsPropagatesInvalidStorageTypes(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "test", JobType: "wrong-type"}
	if err := rc.Set(ctx, backend.delayedKey(topo), "not-a-zset", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Stats(ctx, topo); err == nil {
		t.Fatal("wrong delayed storage type returned successful zero stats")
	}
}

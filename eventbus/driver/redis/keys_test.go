package redis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func hashTag(t *testing.T, key string) string {
	t.Helper()
	start := strings.IndexByte(key, '{')
	end := strings.IndexByte(key, '}')
	if start < 0 || end <= start+1 {
		t.Fatalf("invalid hash tag: %q", key)
	}
	return key[start+1 : end]
}

// Redis Cluster hashes precisely the first nonempty {...} substring. Equal
// tags prove every key used together by future scripts has the same slot.
func TestTopologyKeysShareClusterHashTagAndIsolateTuples(t *testing.T) {
	backend := &redisTransport{}
	topologies := []transport.Topology{
		{Namespace: "a.b", JobType: "c"}, {Namespace: "a", JobType: "b.c"},
		{Namespace: "a", JobType: "event.c"}, {Namespace: "a", JobType: "c", Kind: transport.EventTopology},
		{Namespace: "{forced}:你好", JobType: "a}:b{"}, {Namespace: "", JobType: "c"},
	}
	seen := map[string]bool{}
	for _, topo := range topologies {
		keys := []string{backend.streamKey(topo), backend.delayedKey(topo), backend.dlqKey(topo), backend.metadataKey(topo), backend.groupIndexKey(topo), backend.dedupeKey(topo, "user}:key{"), backend.subscriberRetryStreamKey(topo, "subscriber:user}:key{"), backend.subscriberRetryDelayedKey(topo, "subscriber:user}:key{")}
		tag := hashTag(t, keys[0])
		if seen[tag] {
			t.Fatal("distinct topology tuples share identity")
		}
		seen[tag] = true
		unique := map[string]bool{}
		for _, key := range keys {
			if hashTag(t, key) != tag || unique[key] || strings.Count(key, "{") != 1 || strings.Count(key, "}") != 1 {
				t.Fatal("unsafe or colliding topology key")
			}
			unique[key] = true
		}
	}
}

func TestScopedDedupeIsolationExpiryAndRelease(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rc.Close()
	backend := New(rc).(*redisTransport)
	ctx := context.Background()
	topologies := []transport.Topology{{Namespace: "one", JobType: "job"}, {Namespace: "two", JobType: "job"}, {Namespace: "one", JobType: "other"}, targetedEventTopology("one", "job")}
	for _, topo := range topologies {
		ok, err := backend.ReserveTopology(ctx, topo, "same", time.Minute)
		if err != nil || !ok {
			t.Fatal("reservation crossed topology scope")
		}
		ok, err = backend.ReserveTopology(ctx, topo, "same", time.Minute)
		if err != nil || ok {
			t.Fatal("duplicate not detected")
		}
	}
	if err := backend.ReleaseTopology(ctx, topologies[0], "same"); err != nil {
		t.Fatal(err)
	}
	if ok, err := backend.ReserveTopology(ctx, topologies[1], "same", time.Minute); err != nil || ok {
		t.Fatal("release affected other topology")
	}
	if ok, err := backend.ReserveTopology(ctx, topologies[0], "same", time.Minute); err != nil || !ok {
		t.Fatal("release did not reopen reservation")
	}
	mr.FastForward(time.Minute)
	for _, topo := range topologies {
		if ok, err := backend.ReserveTopology(ctx, topo, "same", time.Minute); err != nil || !ok {
			t.Fatal("reservation did not expire")
		}
	}
}

func TestPublicationDedupeRequiresConfirmedState(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rc.Close()
	backend := New(rc).(*redisTransport)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "dedupe", JobType: "job"}

	state, err := backend.BeginPublication(ctx, topo, "same", "owner-1", time.Minute)
	if err != nil || state != transport.DedupeReservationAcquired {
		t.Fatalf("begin = %d, %v", state, err)
	}
	state, err = backend.BeginPublication(ctx, topo, "same", "owner-2", time.Minute)
	if err != nil || state != transport.DedupeReservationPending {
		t.Fatalf("unconfirmed state = %d, %v", state, err)
	}
	if err := backend.AbortPublication(ctx, topo, "same", "owner-2"); !errors.Is(err, transport.ErrDedupeAmbiguous) {
		t.Fatalf("wrong-owner abort = %v", err)
	}
	if err := backend.ConfirmPublication(ctx, topo, "same", "owner-1"); err != nil {
		t.Fatal(err)
	}
	state, err = backend.BeginPublication(ctx, topo, "same", "owner-3", time.Minute)
	if err != nil || state != transport.DedupeReservationPublished {
		t.Fatalf("confirmed state = %d, %v", state, err)
	}
}

// DS-60: the public producer must select the scoped extension, not just expose
// it for direct driver calls. Tasks and events with equal names remain isolated.
func TestEngineUsesTopologyScopedRedisDedupe(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rc.Close()
	backend := New(rc).(*redisTransport)
	ctx := context.Background()
	for _, kind := range []transport.TopologyKind{transport.TaskTopology, transport.EventTopology} {
		cfg := eventbus.DefaultConfig()
		cfg.Namespace = "scoped"
		bus, err := eventbus.Open(cfg, eventbus.WithTransport(backend))
		if err != nil {
			t.Fatal(err)
		}
		if kind == transport.TaskTopology {
			err = bus.DeclareTask("same")
		} else {
			err = bus.DeclareEvent("same", WithEventLayoutTargeted())
		}
		if err != nil {
			t.Fatal(err)
		}
		topo := transport.Topology{Namespace: cfg.Namespace, Kind: kind, JobType: "same"}
		if kind == transport.EventTopology {
			topo.Options = map[string]any{redisEventLayoutOption: redisTargetedLayoutV1}
			ensureTargetedEventTopology(t, backend, topo)
		}
		publish := bus.Enqueue
		if kind == transport.EventTopology {
			publish = bus.Publish
		}
		for range 2 {
			if err := publish(ctx, "same", 1, eventbus.WithDedupeKey("same")); err != nil {
				t.Fatal(err)
			}
		}
		if n, err := rc.XLen(ctx, backend.streamKey(topo)).Result(); err != nil || n != 1 {
			t.Fatalf("published count = %d, %v", n, err)
		}
		if err := bus.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTopologyMetadataUsesCanonicalKey(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rc.Close()
	backend := New(rc).(*redisTransport)
	defer backend.Close(context.Background())
	topo := targetedEventTopology("namespace", "name")
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	metadata, err := rc.HGetAll(context.Background(), backend.metadataKey(topo)).Result()
	if err != nil || metadata["layout_version"] != "1" || metadata["event_layout"] != redisTargetedLayoutV1 || metadata["namespace"] != topo.Namespace || metadata["kind"] != "event" || metadata["name"] != topo.JobType {
		t.Fatal("metadata not persisted")
	}
}

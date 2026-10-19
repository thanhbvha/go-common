//go:build integration

package nats

import (
	"context"
	"strings"
	"testing"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func targetedEventTopology(namespace, jobType string) transport.Topology {
	return transport.Topology{
		Namespace: namespace,
		JobType:   jobType,
		Kind:      transport.EventTopology,
		Options:   map[string]any{natsEventLayoutOption: natsTargetedLayoutV1},
	}
}

func TestEventLayoutMarkerFencesTargetedPaths(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	ctx := context.Background()
	topo := targetedEventTopology("migration", "fence")
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	kv, err := backend.getKV(backend.kvBucketName(topo))
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Delete(eventLayoutMarkerKey); err != nil {
		t.Fatal(err)
	}

	wantFence := func(name string, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "targeted event layout") {
			t.Fatalf("%s without layout marker = %v", name, err)
		}
	}
	wantFence("publish", backend.Publish(ctx, topo, transport.Message{ID: "id", Payload: []byte("payload")}))
	wantFence("delayed publish", backend.PublishAt(ctx, topo, transport.Message{ID: "delayed", Payload: []byte("payload")}, time.Now().Add(time.Hour)))
	sub := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	wantFence("provision", backend.EnsureSubscription(ctx, sub))
	wantFence("cleanup", backend.CleanupSubscription(ctx, sub))
	_, err = backend.SubscriptionStats(ctx, sub)
	wantFence("subscriber stats", err)
	wantFence("DLQ requeue", backend.Requeue(ctx, topo, nil))
	_, err = backend.dispatchDue(ctx, topo, kv, time.Now())
	wantFence("scheduler", err)
}

func TestEventLayoutMarkerFencesConsume(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	ctx := context.Background()
	topo := targetedEventTopology("migration", "consume_fence")
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	kv, err := backend.getKV(backend.kvBucketName(topo))
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Delete(eventLayoutMarkerKey); err != nil {
		t.Fatal(err)
	}
	err = backend.Consume(ctx, transport.Subscription{Topology: topo, Group: "alpha"}, func(transport.Delivery) {})
	if err == nil || !strings.Contains(err.Error(), "targeted event layout") {
		t.Fatalf("consume without layout marker = %v", err)
	}
}

func TestEnsureTopologyRejectsConflictingEventLayoutMarkerBeforeSchedulerState(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	ctx := context.Background()
	topo := targetedEventTopology("migration", "conflict")
	kv, err := js.CreateKeyValue(&gonats.KeyValueConfig{Bucket: backend.kvBucketName(topo), Storage: gonats.FileStorage})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Create(eventLayoutMarkerKey, []byte("broadcast-v0")); err != nil {
		t.Fatal(err)
	}
	if err := backend.EnsureTopology(ctx, topo); err == nil || !strings.Contains(err.Error(), "marker") {
		t.Fatalf("conflicting marker = %v", err)
	}
	if _, err := kv.Get(delayCursorKey); err == nil {
		t.Fatal("scheduler cursor was created after layout conflict")
	}
}

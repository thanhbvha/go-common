//go:build integration

package nats

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestEnsureTopologyCreatesAndUpdatesManagedStreamConfig(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	ctx := context.Background()
	topo := transport.Topology{
		Namespace: "provision", JobType: "updates",
		Options: map[string]any{"nats.maxage": time.Hour, "nats.retention": gonats.LimitsPolicy},
	}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	mainName, dlqName := backend.streamName(topo), backend.dlqStreamName(topo)
	main, err := js.StreamInfo(mainName)
	if err != nil {
		t.Fatal(err)
	}
	main.Config.MaxMsgs = 77
	if _, err := js.UpdateStream(&main.Config); err != nil {
		t.Fatal(err)
	}

	updated := topo
	updated.Options = map[string]any{"nats.maxage": 2 * time.Hour, "nats.retention": gonats.InterestPolicy}
	if err := backend.EnsureTopology(ctx, updated); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{mainName, dlqName} {
		info, err := js.StreamInfo(name)
		if err != nil {
			t.Fatal(err)
		}
		if info.Config.MaxAge != 2*time.Hour || info.Config.Retention != gonats.InterestPolicy ||
			info.Config.Storage != gonats.FileStorage || info.Config.Replicas != 1 ||
			len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != name {
			t.Fatalf("stream %s config = %+v", name, info.Config)
		}
		if name == mainName && info.Config.MaxMsgs != 77 {
			t.Fatal("reconciliation overwrote an unmanaged stream limit")
		}
	}
}

func TestEnsureTopologyReturnsIncompatibleStreamUpdate(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "provision", JobType: "incompatible"}
	name := backend.streamName(topo)
	if _, err := js.AddStream(&gonats.StreamConfig{Name: name, Subjects: []string{name}, Storage: gonats.MemoryStorage}); err != nil {
		t.Fatal(err)
	}
	if err := backend.EnsureTopology(ctx, topo); err == nil || !strings.Contains(err.Error(), "update stream") {
		t.Fatalf("incompatible stream update = %v", err)
	}
}

func TestConsumerProvisioningUpdatesCapacityAndPreservesConfig(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	ctx := context.Background()
	topo := transport.Topology{
		Namespace: "provision", JobType: "consumer",
		Options: map[string]any{"nats.ack_wait": time.Second},
	}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	stream := backend.streamName(topo)
	sub := transport.Subscription{Topology: topo, Concurrency: 2, Prefetch: 3}
	if err := backend.reconcileConsumer(ctx, stream, sub); err != nil {
		t.Fatal(err)
	}
	name := backend.consumerName(sub)
	info, err := js.ConsumerInfo(stream, name)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxAckPending != 5 || info.Config.AckWait != time.Second {
		t.Fatalf("initial consumer config = %+v", info.Config)
	}
	info.Config.MaxDeliver = 7
	if _, err := js.UpdateConsumer(stream, &info.Config); err != nil {
		t.Fatal(err)
	}

	updatedTopo := topo
	updatedTopo.Options = map[string]any{"nats.ack_wait": 2 * time.Second}
	sub.Topology = updatedTopo
	sub.Concurrency, sub.Prefetch = 4, 7
	if err := backend.reconcileConsumer(ctx, stream, sub); err != nil {
		t.Fatal(err)
	}
	info, err = js.ConsumerInfo(stream, name)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxAckPending != 11 || info.Config.AckWait != 2*time.Second || info.Config.MaxDeliver != 7 {
		t.Fatalf("updated consumer config = %+v", info.Config)
	}
}

func TestConsumerProvisioningRejectsIncompatibleMode(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "provision", JobType: "consumer_mode"}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Concurrency: 1, Prefetch: 1}
	name := backend.consumerName(sub)
	if _, err := js.AddConsumer(backend.streamName(topo), &gonats.ConsumerConfig{Durable: name, AckPolicy: gonats.AckNonePolicy}); err != nil {
		t.Fatal(err)
	}
	if err := backend.reconcileConsumer(ctx, backend.streamName(topo), sub); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("incompatible consumer = %v", err)
	}
}

func TestConcurrentTopologyAndConsumerProvisioningIsIdempotent(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	ctx := context.Background()
	topo := transport.Topology{Namespace: "provision", JobType: "concurrent", Options: map[string]any{"nats.ack_wait": time.Second}}

	runConcurrent := func(operation func() error) {
		t.Helper()
		const instances = 8
		var wg sync.WaitGroup
		errs := make(chan error, instances)
		for range instances {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- operation()
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	runConcurrent(func() error { return backend.EnsureTopology(ctx, topo) })
	sub := transport.Subscription{Topology: topo, Concurrency: 3, Prefetch: 4}
	runConcurrent(func() error { return backend.reconcileConsumer(ctx, backend.streamName(topo), sub) })
	info, err := js.ConsumerInfo(backend.streamName(topo), backend.consumerName(sub))
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxAckPending != 7 || info.Config.AckWait != time.Second {
		t.Fatalf("concurrently provisioned consumer = %+v", info.Config)
	}
}

func provisioningJetStream(t *testing.T) (*server.Server, gonats.JetStreamContext) {
	t.Helper()
	s := RunServerOnPort(t, server.RANDOM_PORT)
	nc, err := gonats.Connect(s.ClientURL())
	if err != nil {
		s.Shutdown()
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		s.Shutdown()
		t.Fatal(err)
	}
	return s, js
}

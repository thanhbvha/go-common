//go:build integration

package nats

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestDedupeIsTopologyScopedAndHonorsPerReservationTTL(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	ctx := context.Background()
	topologies := []transport.Topology{
		{Namespace: "one", JobType: "job"},
		{Namespace: "two", JobType: "job"},
		{Namespace: "one", JobType: "other"},
		{Namespace: "one", JobType: "job", Kind: transport.EventTopology},
	}
	for _, topo := range topologies {
		if err := backend.EnsureTopology(ctx, topo); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(1_700_000_000, 0)
	backend.now = func() time.Time { return now }
	for _, topo := range topologies {
		if ok, err := backend.ReserveTopology(ctx, topo, "same", time.Second); err != nil || !ok {
			t.Fatalf("first reservation for %+v = %v, %v", topo, ok, err)
		}
		if ok, err := backend.ReserveTopology(ctx, topo, "same", time.Minute); err != nil || ok {
			t.Fatalf("duplicate reservation for %+v = %v, %v", topo, ok, err)
		}
	}
	now = now.Add(time.Second)
	for _, topo := range topologies {
		if ok, err := backend.ReserveTopology(ctx, topo, "same", 5*time.Minute); err != nil || !ok {
			t.Fatalf("expired reservation for %+v = %v, %v", topo, ok, err)
		}
	}
	if err := backend.ReleaseTopology(ctx, topologies[0], "same"); err != nil {
		t.Fatal(err)
	}
	if ok, err := backend.ReserveTopology(ctx, topologies[0], "same", time.Minute); err != nil || !ok {
		t.Fatalf("released reservation = %v, %v", ok, err)
	}
}

func TestExpiredDedupeReservationHasSingleCASWinner(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	backend := New(js).(*natsTransport)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "cas", JobType: "job"}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	backend.now = func() time.Time { return now }
	if ok, err := backend.ReserveTopology(ctx, topo, "same", time.Second); err != nil || !ok {
		t.Fatal("initial reservation failed")
	}
	now = now.Add(time.Second)

	const contenders = 16
	results := make(chan bool, contenders)
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := backend.ReserveTopology(ctx, topo, "same", time.Minute)
			results <- ok
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	winners := 0
	for ok := range results {
		if ok {
			winners++
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if winners != 1 {
		t.Fatalf("CAS winners = %d, want 1", winners)
	}
}

func TestDedupeDistinguishesCorruptionAndInfrastructureFailureFromDuplicate(t *testing.T) {
	s := RunServerOnPort(t, server.RANDOM_PORT)
	defer s.Shutdown()
	nc, err := gonats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	backend := New(js).(*natsTransport)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "errors", JobType: "job"}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	kv, err := js.KeyValue(dedupeBucket)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(dedupeStorageKey(&topo, "corrupt"), []byte("not-a-timestamp")); err != nil {
		t.Fatal(err)
	}
	if ok, err := backend.ReserveTopology(ctx, topo, "corrupt", time.Minute); err == nil || ok {
		t.Fatalf("corrupt reservation = %v, %v; want infrastructure/data error", ok, err)
	}
	nc.Close()
	if ok, err := backend.ReserveTopology(ctx, topo, "unavailable", time.Minute); err == nil || ok {
		t.Fatalf("closed connection reservation = %v, %v; want infrastructure error", ok, err)
	}
}

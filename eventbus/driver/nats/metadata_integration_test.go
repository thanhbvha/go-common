//go:build integration

package nats

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

// Regression: consumed NATS deliveries have no transport ID. Before P2.7,
// retrying two distinct jobs at one deadline wrote the same "timestamp_" KV
// key, overwriting one accepted job. Exercise the real engine and KV writer.
func TestEngineRetriesPreserveIDsAtSameDeadline(t *testing.T) {
	s := RunServerOnPort(t, server.RANDOM_PORT)
	defer s.Shutdown()
	nc, err := gonats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	backend := New(js).(*natsTransport)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	topo := transport.Topology{Namespace: fmt.Sprintf("metadata_%d", time.Now().UnixNano()), JobType: "retry_ids"}
	runAt := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	codec := eventbus.NewCodec(nil)
	want := make(map[string]bool)
	for range 2 {
		_, env, err := codec.EncodeTopology(eventbus.TaskTopology, topo.JobType, 1, 2)
		if err != nil {
			t.Fatal(err)
		}
		want[env.ID] = true
	}
	// Clamp the destination time so both retries exercise the exact same key
	// prefix, regardless of handler scheduling or backoff clock granularity.
	probe := &sameDeadlineTransport{Transport: backend, runAt: runAt, settled: make(chan error, 2)}
	cfg := eventbus.DefaultConfig()
	cfg.Namespace = topo.Namespace
	cfg.Concurrency = 1
	bus, err := eventbus.Open(cfg, eventbus.WithTransport(probe))
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Close(context.Background())
	if err := bus.Register(topo.JobType, func(context.Context, eventbus.Job) error {
		return eventbus.MarkRetryAfter(fmt.Errorf("synthetic retry"), time.Hour)
	}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for id := range want {
		data := []byte(fmt.Sprintf(`{"v":3,"id":%q,"kind":0,"type":%q,"payload":1,"attempt":1,"max_attempts":2,"headers":{"correlation-id":"synthetic"}}`, id, topo.JobType))
		if err := backend.Publish(ctx, topo, transport.Message{ID: id, Payload: data}); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case err := <-probe.settled:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("retry settlement timed out")
		}
	}
	if err := bus.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	kv, err := backend.getKV(backend.kvBucketName(topo))
	if err != nil {
		t.Fatal(err)
	}
	keys, err := kv.Keys()
	if err != nil {
		t.Fatal(err)
	}
	delayed := 0
	for _, key := range keys {
		if !strings.HasPrefix(key, "v2.") {
			continue
		}
		delayed++
		entry, err := kv.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		var record delayRecord
		if err := json.Unmarshal(entry.Value(), &record); err != nil {
			t.Fatal(err)
		}
		env, err := codec.Decode(record.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if !want[env.ID] || record.MessageID != env.ID || record.RunAt != runAt.UnixMilli() || env.Attempt != 2 || env.Headers["correlation-id"] != "synthetic" {
			t.Fatal("retry identity/metadata corrupted")
		}
		delete(want, env.ID)
	}
	if delayed != 2 {
		t.Fatalf("delayed records = %d, want 2 (IDs collided)", delayed)
	}
}

type sameDeadlineTransport struct {
	transport.Transport
	runAt   time.Time
	settled chan error
}

func (p *sameDeadlineTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	return p.Transport.Consume(ctx, sub, func(d transport.Delivery) { deliver(&sameDeadlineDelivery{Delivery: d, probe: p}) })
}

type sameDeadlineDelivery struct {
	transport.Delivery
	probe *sameDeadlineTransport
}

func (d *sameDeadlineDelivery) Settle(ctx context.Context, decision transport.Decision) error {
	decision.RetryAt = d.probe.runAt
	err := d.Delivery.Settle(ctx, decision)
	d.probe.settled <- err
	return err
}

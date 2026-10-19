//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestEncodedTopologyNamesPreventKafkaCrossDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	producer, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.Ping(ctx); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	backend := New(producer, nil).(*kafkaTransport)
	first := transport.Topology{Namespace: "acme", JobType: "event.foo", Kind: transport.TaskTopology}
	second := transport.Topology{Namespace: "acme", JobType: "foo", Kind: transport.EventTopology}
	if backend.mainTopic(first) == backend.mainTopic(second) {
		t.Fatal("hostile task/event tuples share a Kafka topic")
	}
	for _, topo := range []transport.Topology{first, second} {
		if err := backend.EnsureTopology(ctx, topo); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_ = backend.Close(context.Background())
		var topics []string
		for _, topo := range []transport.Topology{first, second} {
			topics = append(topics, backend.mainTopic(topo), backend.dlqTopic(topo))
			topics = append(topics, backend.delayTopics(topo)...)
		}
		_, _ = backend.adm.DeleteTopics(context.Background(), topics...)
	})
	if err := backend.Publish(ctx, first, transport.Message{ID: "task-id", Payload: []byte("task")}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(ctx, second, transport.Message{ID: "event-id", Payload: []byte("event")}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		topo    transport.Topology
		payload string
	}{{first, "task"}, {second, "event"}} {
		consumer, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumeTopics(backend.mainTopic(want.topo)),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		if err != nil {
			t.Fatal(err)
		}
		fetchCtx, fetchCancel := context.WithTimeout(ctx, 10*time.Second)
		fetches := consumer.PollFetches(fetchCtx)
		fetchCancel()
		consumer.Close()
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		records := fetches.Records()
		if len(records) != 1 || string(records[0].Value) != want.payload {
			t.Fatalf("topic %q records = %d payload=%q, want one %q", backend.mainTopic(want.topo), len(records), firstPayload(records), want.payload)
		}
	}
}

func firstPayload(records []*kgo.Record) string {
	if len(records) == 0 {
		return ""
	}
	return string(records[0].Value)
}

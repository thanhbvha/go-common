//go:build integration

package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaTransport_PublishAndConsume(t *testing.T) {
	// Create base kgo.Client
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	// Need a quick ping to see if kafka is available
	if err := cl.Ping(context.Background()); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}

	trans := New(cl, []string{"localhost:9092"})
	ctx := context.Background()
	topo := transport.Topology{Namespace: "testns", JobType: fmt.Sprintf("testjob_kafka_%d", time.Now().UnixNano())}

	err = trans.EnsureTopology(ctx, topo)
	if err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	// Wait a moment for topic creation to propagate
	time.Sleep(1 * time.Second)

	// Publish
	err = trans.Publish(ctx, topo, transport.Message{
		Payload: []byte("hello kafka"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Consume
	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	received := make(chan transport.Delivery)

	go func() {
		err := trans.Consume(consumeCtx, transport.Subscription{Topology: topo}, func(d transport.Delivery) {
			received <- d
		})
		if err != nil && err != context.Canceled {
			t.Errorf("Consume error: %v", err)
		}
	}()

	select {
	case d := <-received:
		msg := d.Message()
		if string(msg.Payload) != "hello kafka" {
			t.Fatalf("Expected payload, got %s", msg.Payload)
		}
		// Ack
		err = d.Settle(ctx, transport.Decision{Kind: transport.DecisionAck})
		if err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("Timeout waiting for message")
	}

	trans.Close(ctx)
}

func TestKafkaTransport_Delay(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	if err := cl.Ping(context.Background()); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}

	trans := New(cl, []string{"localhost:9092"})
	ctx := context.Background()
	topo := transport.Topology{Namespace: "testns", JobType: fmt.Sprintf("testjob_delay_%d", time.Now().UnixNano())}

	trans.EnsureTopology(ctx, topo)
	time.Sleep(1 * time.Second)

	// Since Envelope is parsed in the delay consumer to find RunAt, we must serialize a proper Envelope
	// But kafkaTransport doesn't use the JSON's RunAt directly for Pause!
	// Wait, in `kafka.go`, delayConsumer does: `if err := json.Unmarshal(record.Value, &env); err != nil { ... cl.MarkCommitRecords }`
	// It relies on `eventbus.Envelope`! So we MUST publish an `eventbus.Envelope` JSON.

	payload := `{"id":"123","type":"testjob_kafka_delay","payload":"\"hello\"","run_at":` + fmt.Sprintf("%d", time.Now().Add(3*time.Second).UnixMilli()) + `}`

	err = trans.PublishAt(ctx, topo, transport.Message{
		Payload: []byte(payload),
	}, time.Now().Add(3*time.Second))

	if err != nil {
		t.Fatalf("PublishAt: %v", err)
	}

	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	received := make(chan transport.Delivery)

	go func() {
		_ = trans.Consume(consumeCtx, transport.Subscription{Topology: topo}, func(d transport.Delivery) {
			received <- d
		})
	}()

	start := time.Now()
	select {
	case d := <-received:
		dur := time.Since(start)
		if dur < 2*time.Second {
			t.Fatalf("Message received too early! dur=%v", dur)
		}
		// Ack
		d.Settle(ctx, transport.Decision{Kind: transport.DecisionAck})
	case <-time.After(8 * time.Second):
		t.Fatalf("Timeout waiting for delayed message")
	}

	trans.Close(ctx)
}

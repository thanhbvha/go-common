package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestDeliveryAllowsOnlyOneTerminalSettlement(t *testing.T) {
	backend := New()
	topo := transport.Topology{Namespace: "test", JobType: "single-settlement"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer backend.Close(context.Background())

	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	deliveries := make(chan transport.Delivery, 1)
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- backend.Consume(ctx, transport.Subscription{Topology: topo}, func(delivery transport.Delivery) {
			deliveries <- delivery
		})
	}()

	message := transport.Message{ID: "message-id", Payload: []byte(`{"v":3}`)}
	if err := backend.Publish(ctx, topo, message); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var delivery transport.Delivery
	select {
	case delivery = <-deliveries:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delivery")
	}
	if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
		t.Fatalf("first settlement: %v", err)
	}
	if err := delivery.Settle(ctx, transport.Decision{
		Kind:    transport.DecisionRetry,
		Next:    message,
		RetryAt: time.Now(),
	}); !errors.Is(err, transport.ErrDeliverySettled) {
		t.Fatalf("second settlement error = %v, want ErrDeliverySettled", err)
	}

	cancel()
	select {
	case <-consumeDone:
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop")
	}
}

func TestDLQUsesNormalizedSourceMetadata(t *testing.T) {
	backend := New().(*memoryTransport)
	defer backend.Close(context.Background())
	topo := transport.Topology{Namespace: "test", JobType: "metadata"}
	source := transport.Message{ID: "envelope-id", Type: "metadata", OrderingKey: "ordering", DedupeKey: "dedupe", Headers: map[string]string{"correlation-id": "synthetic"}, Payload: []byte(`{"v":3}`)}
	delivery := &memoryDelivery{transport: backend, topo: topo, msg: transport.Message{ID: "stale"}}
	if err := delivery.Settle(context.Background(), transport.Decision{Kind: transport.DecisionDeadLetter, Source: &source, Reason: "synthetic"}); err != nil {
		t.Fatal(err)
	}
	records, err := backend.List(context.Background(), topo, 0, 10)
	if err != nil || len(records) != 1 {
		t.Fatal("missing DLQ record")
	}
	msg := records[0].Message
	if msg.ID != source.ID || msg.Type != source.Type || msg.OrderingKey != source.OrderingKey || msg.DedupeKey != source.DedupeKey || msg.Headers["correlation-id"] != "synthetic" {
		t.Fatal("DLQ used raw broker metadata")
	}
}

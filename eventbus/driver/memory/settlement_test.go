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
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(context.Background(), topo, transport.Message{ID: "stale", Payload: source.Payload}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deliveries := make(chan transport.Delivery, 1)
	go func() {
		_ = backend.Consume(ctx, transport.Subscription{Topology: topo}, func(delivery transport.Delivery) {
			deliveries <- delivery
		})
	}()
	delivery := <-deliveries
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

func TestDLQListDoesNotExposeStoredMessage(t *testing.T) {
	backend := New().(*memoryTransport)
	defer backend.Close(context.Background())
	topo := transport.Topology{Namespace: "test", JobType: "dlq-isolation"}
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(context.Background(), topo, transport.Message{Payload: []byte("original"), Headers: map[string]string{"trace": "original"}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deliveries := make(chan transport.Delivery, 1)
	go func() {
		_ = backend.Consume(ctx, transport.Subscription{Topology: topo}, func(delivery transport.Delivery) { deliveries <- delivery })
	}()
	delivery := <-deliveries
	if err := delivery.Settle(context.Background(), transport.Decision{Kind: transport.DecisionDeadLetter, Reason: "test"}); err != nil {
		t.Fatal(err)
	}
	records, err := backend.List(context.Background(), topo, 0, 1)
	if err != nil || len(records) != 1 {
		t.Fatalf("records=%#v err=%v", records, err)
	}
	records[0].Message.Payload[0] = 'X'
	records[0].Message.Headers["trace"] = "mutated"
	again, err := backend.List(context.Background(), topo, 0, 1)
	if err != nil || string(again[0].Message.Payload) != "original" || again[0].Message.Headers["trace"] != "original" {
		t.Fatalf("stored DLQ message mutated = %#v, %v", again, err)
	}
}

func TestMemoryDLQSanitizesMetadataAndBoundsRecord(t *testing.T) {
	backend := New().(*memoryTransport)
	defer backend.Close(context.Background())
	topo := transport.Topology{Namespace: "test", JobType: "dlq-safety"}
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	oversized := transport.Message{Payload: make([]byte, transport.MaxEncodedDLQRecordBytes+1)}
	if err := backend.Publish(context.Background(), topo, oversized); err != nil {
		t.Fatal(err)
	}
	deliveries := make(chan transport.Delivery, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = backend.Consume(ctx, transport.Subscription{Topology: topo}, func(d transport.Delivery) { deliveries <- d })
	}()
	delivery := <-deliveries
	decision := transport.Decision{Kind: transport.DecisionDeadLetter, Reason: "contains secret", ErrorCode: "also secret", Cause: hostileMemoryError{}}
	if err := delivery.Settle(context.Background(), decision); err == nil {
		t.Fatal("oversized DLQ record unexpectedly settled")
	}
}

type hostileMemoryError struct{}

func (hostileMemoryError) Error() string { panic("must not render") }

func TestMemoryPublicationDedupeRequiresConfirmedState(t *testing.T) {
	backend := New().(*memoryTransport)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "dedupe", JobType: "task"}
	state, err := backend.BeginPublication(ctx, topo, "same", "owner-1", time.Minute)
	if err != nil || state != transport.DedupeReservationAcquired {
		t.Fatalf("begin = %d, %v", state, err)
	}
	state, err = backend.BeginPublication(ctx, topo, "same", "owner-2", time.Minute)
	if err != nil || state != transport.DedupeReservationPending {
		t.Fatalf("pending = %d, %v", state, err)
	}
	if err := backend.ConfirmPublication(ctx, topo, "same", "owner-1"); err != nil {
		t.Fatal(err)
	}
	state, err = backend.BeginPublication(ctx, topo, "same", "owner-3", time.Minute)
	if err != nil || state != transport.DedupeReservationPublished {
		t.Fatalf("published = %d, %v", state, err)
	}
}

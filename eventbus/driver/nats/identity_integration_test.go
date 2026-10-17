//go:build integration

package nats

import (
	"context"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestPublishUsesMessageIDNotOrderingKeyForJetStreamIdempotency(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	backend := New(js).(*natsTransport)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "identity", JobType: "immediate"}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	for _, msg := range []transport.Message{
		{ID: "message-1", OrderingKey: "shared", Payload: []byte("one")},
		{ID: "message-2", OrderingKey: "shared", Payload: []byte("two")},
		{ID: "message-1", OrderingKey: "different", Payload: []byte("duplicate")},
	} {
		if err := backend.Publish(ctx, topo, msg); err != nil {
			t.Fatal(err)
		}
	}
	info, err := js.StreamInfo(backend.streamName(topo))
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 2 {
		t.Fatalf("stored messages = %d, want 2", info.State.Msgs)
	}

	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ids := make(chan string, 2)
	errs := make(chan error, 1)
	go func() {
		errs <- backend.Consume(consumeCtx, transport.Subscription{Topology: topo}, func(d transport.Delivery) {
			ids <- d.Message().ID
			if err := d.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
				t.Errorf("ack: %v", err)
			}
		})
	}()
	want := map[string]bool{"message-1": true, "message-2": true}
	for range 2 {
		select {
		case id := <-ids:
			if !want[id] {
				t.Fatalf("delivered ID = %q", id)
			}
			delete(want, id)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for immediate delivery")
		}
	}
	cancel()
	select {
	case <-errs:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
	}
}

func TestMessageIDSurvivesRetryDelayAndDeadLetter(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	backend := New(js).(*natsTransport)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "identity", JobType: "lifecycle"}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	const id = "logical-message-id"
	if err := backend.Publish(ctx, topo, transport.Message{ID: id, Payload: []byte("attempt-1")}); err != nil {
		t.Fatal(err)
	}

	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	seen := make(chan string, 2)
	settled := make(chan error, 2)
	deliveries := 0
	go func() {
		_ = backend.Consume(consumeCtx, transport.Subscription{Topology: topo}, func(d transport.Delivery) {
			deliveries++
			seen <- d.Message().ID
			if deliveries == 1 {
				settled <- d.Settle(ctx, transport.Decision{
					Kind:    transport.DecisionRetry,
					Next:    transport.Message{ID: id, Payload: []byte("attempt-2")},
					RetryAt: time.Now().Add(1100 * time.Millisecond),
				})
				return
			}
			settled <- d.Settle(ctx, transport.Decision{Kind: transport.DecisionDeadLetter})
			cancel()
		})
	}()
	for range 2 {
		select {
		case got := <-seen:
			if got != id {
				t.Fatalf("lifecycle delivery ID = %q, want %q", got, id)
			}
		case <-time.After(7 * time.Second):
			t.Fatal("timed out waiting for retry lifecycle")
		}
		if err := <-settled; err != nil {
			t.Fatal(err)
		}
	}
	stored, err := js.GetMsg(backend.dlqStreamName(topo), 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.Header.Get(messageIDHeader); got != id {
		t.Fatalf("DLQ logical ID = %q, want %q", got, id)
	}
}

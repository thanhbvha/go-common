package eventbus

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestPublishEnforcesSerializedEnvelopeBoundary(t *testing.T) {
	payload := map[string]string{"value": "boundary"}
	sample, _, err := NewCodec(nil).EncodeTopology(TaskTopology, "limited", payload, 5)
	if err != nil {
		t.Fatalf("sample encode: %v", err)
	}

	t.Run("exact limit", func(t *testing.T) {
		backend := &benchmarkTransport{}
		cfg := DefaultConfig()
		cfg.MaxPayloadBytes = len(sample)
		instance, openErr := Open(cfg, WithTransport(backend))
		if openErr != nil {
			t.Fatalf("Open: %v", openErr)
		}
		if declareErr := instance.DeclareTask("limited"); declareErr != nil {
			t.Fatalf("DeclareTask: %v", declareErr)
		}
		if enqueueErr := instance.Enqueue(context.Background(), "limited", payload); enqueueErr != nil {
			t.Fatalf("Enqueue at exact limit: %v", enqueueErr)
		}
		if got := backend.published.Load(); got != 1 {
			t.Fatalf("published = %d, want 1", got)
		}
	})

	t.Run("one byte over", func(t *testing.T) {
		backend := &benchmarkTransport{}
		cfg := DefaultConfig()
		cfg.MaxPayloadBytes = len(sample) - 1
		instance, openErr := Open(cfg, WithTransport(backend))
		if openErr != nil {
			t.Fatalf("Open: %v", openErr)
		}
		if declareErr := instance.DeclareTask("limited"); declareErr != nil {
			t.Fatalf("DeclareTask: %v", declareErr)
		}
		if enqueueErr := instance.Enqueue(context.Background(), "limited", payload); !errors.Is(enqueueErr, ErrPayloadTooLarge) {
			t.Fatalf("Enqueue error = %v, want ErrPayloadTooLarge", enqueueErr)
		}
		if got := backend.published.Load(); got != 0 {
			t.Fatalf("published oversized envelopes = %d, want 0", got)
		}
	})
}

func TestTopologyPayloadLimitOverridesGlobalLimit(t *testing.T) {
	payload := map[string]string{"value": "topology override"}
	sample, _, err := NewCodec(nil).EncodeTopology(TaskTopology, "limited", payload, 5)
	if err != nil {
		t.Fatalf("sample encode: %v", err)
	}
	backend := &benchmarkTransport{}
	cfg := DefaultConfig()
	cfg.MaxPayloadBytes = 1
	instance, err := Open(cfg, WithTransport(backend))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := instance.DeclareTask("limited", WithMaxPayloadBytes(len(sample))); err != nil {
		t.Fatalf("DeclareTask: %v", err)
	}
	if err := instance.Enqueue(context.Background(), "limited", payload); err != nil {
		t.Fatalf("Enqueue with topology override: %v", err)
	}
}

type payloadLimitDelivery struct {
	message  transport.Message
	decision transport.Decision
	settled  atomic.Bool
}

func (d *payloadLimitDelivery) Message() transport.Message { return d.message }
func (*payloadLimitDelivery) DeliveryAttempt() int         { return 2 }
func (*payloadLimitDelivery) Heartbeat(context.Context) error {
	return nil
}
func (d *payloadLimitDelivery) Settle(_ context.Context, decision transport.Decision) error {
	d.decision = decision
	d.settled.Store(true)
	return nil
}

func TestOversizedDeliveryGoesToDLQBeforeDecode(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxPayloadBytes = 3
	eng := newEngine(cfg, &benchmarkTransport{}, NewCodec(nil))
	delivery := &payloadLimitDelivery{message: transport.Message{ID: "raw-id", Payload: []byte("not-json")}}
	var handled atomic.Bool

	eng.processDelivery(TaskTopology, "limited", "", func(context.Context, Job) error {
		handled.Store(true)
		return nil
	}, delivery, 1)

	if handled.Load() {
		t.Fatal("handler ran for oversized delivery")
	}
	if !delivery.settled.Load() {
		t.Fatal("oversized delivery was not settled")
	}
	if delivery.decision.Kind != transport.DecisionDeadLetter || delivery.decision.Reason != "payload_too_large" {
		t.Fatalf("decision = %#v, want payload_too_large DLQ", delivery.decision)
	}
	if !errors.Is(delivery.decision.Cause, ErrPayloadTooLarge) {
		t.Fatalf("decision cause = %v, want ErrPayloadTooLarge", delivery.decision.Cause)
	}
}

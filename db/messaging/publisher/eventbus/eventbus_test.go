package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	bus "github.com/thanhbvha/go-common/v2/eventbus"
	"github.com/thanhbvha/go-common/v2/eventbus/driver/memory"
	"github.com/thanhbvha/go-common/v2/eventbus/transport"
)

type fakeBus struct {
	bus.Bus
	caps      transport.Capabilities
	options   bus.JobOptions
	payload   any
	event     bool
	badResult bool
}

func (f *fakeBus) Capabilities() (transport.Capabilities, error) { return f.caps, nil }
func (f *fakeBus) publish(payload any, opts []bus.JobOption) (bus.PublicationResult, error) {
	f.options = bus.JobOptions{}
	for _, o := range opts {
		o(&f.options)
	}
	f.payload = payload
	id := f.options.MessageID
	if f.badResult {
		id = "wrong"
	}
	return bus.PublicationResult{ID: id}, nil
}
func (f *fakeBus) EnqueueWithResult(_ context.Context, _ string, payload any, opts ...bus.JobOption) (bus.PublicationResult, error) {
	return f.publish(payload, opts)
}
func (f *fakeBus) PublishWithResult(_ context.Context, _ string, payload any, opts ...bus.JobOption) (bus.PublicationResult, error) {
	f.event = true
	return f.publish(payload, opts)
}
func sample() messaging.Message {
	return messaging.Message{ID: "same-id", Namespace: "test", TenantID: "a", Kind: messaging.Task, Type: "created", SchemaVersion: 2, Payload: json.RawMessage(`{"id":9223372036854775807}`), Headers: map[string]string{"traceparent": "test-trace"}}
}
func TestPublisherMetadataCapabilitiesAndScopedIDs(t *testing.T) {
	f := &fakeBus{caps: transport.Capabilities{Durability: transport.DurabilityBroker, Dedupe: transport.DedupeBrokerConfirmed}}
	p, err := NewPublisher(f, Config{Namespace: "test", DedupeTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	m := sample()
	if err := p.Publish(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	if f.options.MessageID != WireID(m) || f.options.Headers[HeaderMessageID] != m.ID || f.options.Headers[HeaderTenant] != "a" || f.options.Headers[HeaderSchemaVersion] != "2" || f.options.Headers["traceparent"] != "test-trace" || f.options.DedupeKey == "" {
		t.Fatal(f.options)
	}
	if got, ok := f.payload.(json.RawMessage); !ok || string(got) != string(m.Payload) {
		t.Fatal("raw payload lost")
	}
	wire := f.options.MessageID
	m.TenantID = "b"
	if err := p.Publish(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	if wire == f.options.MessageID {
		t.Fatal("tenant wire ID collision")
	}
	m.Kind = messaging.Event
	if err := p.Publish(t.Context(), m); err != nil || !f.event {
		t.Fatal("event path", err)
	}
	m.OrderingKey = "key"
	if err := p.Publish(t.Context(), m); !errors.Is(err, bus.ErrNotSupported) {
		t.Fatal(err)
	}
	m.OrderingKey = ""
	f.badResult = true
	if err := p.Publish(t.Context(), m); !errors.Is(err, messaging.ErrIdentityConflict) {
		t.Fatal(err)
	}
	f.badResult = false
	m.Headers[HeaderTenant] = "spoof"
	if err := p.Publish(t.Context(), m); !errors.Is(err, messaging.ErrInvalidMessage) {
		t.Fatal(err)
	}
	f.caps.Durability = transport.DurabilityProcess
	if _, err := NewPublisher(f, Config{Namespace: "test"}); !errors.Is(err, bus.ErrNotSupported) {
		t.Fatal(err)
	}
}

func TestRejectMemoryTransport(t *testing.T) {
	cfg := bus.DefaultConfig()
	b, err := bus.Open(cfg, bus.WithTransport(memory.New()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(t.Context())
	if _, err := NewPublisher(b, Config{Namespace: "test"}); !errors.Is(err, bus.ErrNotSupported) {
		t.Fatal(err)
	}
}

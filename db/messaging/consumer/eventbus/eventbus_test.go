package eventbus

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/internal/eventbuswire"
	bus "github.com/thanhbvha/go-common/v2/eventbus"
	"github.com/thanhbvha/go-common/v2/eventbus/driver/memory"
)

func sample() messaging.Message {
	return messaging.Message{ID: "same-id", Namespace: "test", TenantID: "a", Kind: messaging.Task, Type: "created", SchemaVersion: 2, Payload: json.RawMessage(`{"id":9223372036854775807}`), Headers: map[string]string{"traceparent": "test-trace"}}
}

type processor struct {
	receipts chan inbox.Receipt
	err      error
}

func (p *processor) Consume(ctx context.Context, r inbox.Receipt, fn func(int) error) (inbox.Result, error) {
	p.receipts <- r
	if p.err != nil {
		return "", p.err
	}
	return inbox.Applied, fn(42)
}
func TestInboxWrapperPreservesBusinessIdentityAndTransactionType(t *testing.T) {
	cfg := bus.DefaultConfig()
	cfg.Namespace = "test"
	cfg.MaxAttempts = 1
	b, err := bus.Open(cfg, bus.WithTransport(memory.New()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	})
	p := &processor{receipts: make(chan inbox.Receipt, 2)}
	handled := make(chan struct{}, 1)
	h, err := WrapInbox(p, Config{Namespace: "test", Consumer: "consumer", Tenant: func(context.Context, bus.Job) (string, error) { return "a", nil }}, func(_ context.Context, tx int, job bus.Job) error {
		if tx != 42 || job.Header(HeaderMessageID) != "same-id" {
			t.Error("transaction/identity changed")
		}
		handled <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.DeclareTask("created", bus.WithDelayDisabled()); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleTask("created", h); err != nil {
		t.Fatal(err)
	}
	if err := b.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	m := sample()
	if err := b.Enqueue(t.Context(), m.Type, m.Payload, bus.WithMessageID(eventbuswire.WireID(m)), bus.WithHeader(HeaderMessageID, m.ID), bus.WithHeader(HeaderNamespace, m.Namespace), bus.WithHeader(HeaderTenant, m.TenantID), bus.WithHeader(HeaderSchemaVersion, "2")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handled:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not run")
	}
	r := <-p.receipts
	hash, _ := m.Fingerprint()
	if r.Fingerprint != hash || r.ID != m.ID || r.Consumer != "consumer" {
		t.Fatal(r)
	}
}

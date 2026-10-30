package eventbus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	bus "github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/driver/memory"
	"github.com/thanhbvha/go-common/eventbus/transport"
	bridge "github.com/thanhbvha/go-common/websocket/integration/eventbus"
	"github.com/thanhbvha/go-common/websocket/routing"
)

// Hide optional deduper interfaces to exercise a Kafka-like publication contract.
type noDedupeTransport struct{ transport.Transport }

func (t noDedupeTransport) Capabilities() transport.Capabilities {
	c := t.Transport.Capabilities()
	c.Dedupe = transport.DedupeNone
	return c
}

func TestBridgePublicationDedupePolicy(t *testing.T) {
	for _, policy := range []bridge.PublicationDedupePolicy{bridge.DedupeRequired, bridge.DedupeDisabled} {
		t.Run(map[bridge.PublicationDedupePolicy]string{bridge.DedupeRequired: "required", bridge.DedupeDisabled: "disabled"}[policy], func(t *testing.T) {
			b, err := bus.Open(bus.DefaultConfig(), bus.WithTransport(noDedupeTransport{memory.New()}))
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close(context.Background())
			integration, err := bridge.New(bridge.Config{Bus: b, PublicationDedupe: policy})
			if policy == bridge.DedupeRequired {
				if !errors.Is(err, bus.ErrDedupeUnavailable) {
					t.Fatalf("expected startup capability failure: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			commands := make(chan routing.BusinessCommand, 1)
			notifications := make(chan bridge.Notification, 1)
			ids := make(chan string, 2)
			if err = b.HandleTask("ws.command", func(_ context.Context, j bus.Job) error {
				var c routing.BusinessCommand
				if err := j.Decode(&c); err != nil {
					return err
				}
				commands <- c
				ids <- j.ID()
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err = b.Subscribe("ws.notification", "observer", func(_ context.Context, j bus.Job) error {
				var n bridge.Notification
				if err := j.Decode(&n); err != nil {
					return err
				}
				notifications <- n
				ids <- j.ID()
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err = b.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err = integration.PublishCommand(ctx, routing.BusinessCommand{MessageID: "command-id", TenantID: "t", UserID: "u", SessionID: "s", Type: "business", Payload: []byte(`{}`)}); err != nil {
				t.Fatal(err)
			}
			select {
			case c := <-commands:
				if c.MessageID != "command-id" {
					t.Fatal("command identity changed")
				}
			case <-ctx.Done():
				t.Fatal("command missing")
			}
			if err = integration.PublishNotification(ctx, bridge.Notification{MessageID: "event-id", TenantID: "t", UserID: "u", Payload: []byte("payload")}); err != nil {
				t.Fatal(err)
			}
			select {
			case n := <-notifications:
				if n.MessageID != "event-id" {
					t.Fatal("notification identity changed")
				}
			case <-ctx.Done():
				t.Fatal("event missing")
			}
			got := map[string]bool{}
			for i := 0; i < 2; i++ {
				select {
				case id := <-ids:
					got[id] = true
				case <-ctx.Done():
					t.Fatal("envelope identity missing")
				}
			}
			if !got["command-id"] || !got["event-id"] {
				t.Fatalf("stable EventBus IDs lost: %v", got)
			}
		})
	}
}
func TestBridgeRejectsUnknownDedupePolicy(t *testing.T) {
	b, err := bus.Open(bus.DefaultConfig(), bus.WithTransport(memory.New()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(context.Background())
	if _, err = bridge.New(bridge.Config{Bus: b, PublicationDedupe: 99}); !errors.Is(err, routing.ErrInvalid) {
		t.Fatalf("unknown policy: %v", err)
	}
}

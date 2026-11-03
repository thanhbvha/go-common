//go:build integration

package natspubsub

import (
	"context"
	"os"
	"testing"
	"time"

	commonnats "github.com/thanhbvha/go-common/v2/nats"
	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
)

func TestNATSIntegrationPublishSubscribe(t *testing.T) {
	if os.Getenv("WS_NATS_INTEGRATION") == "" {
		if os.Getenv("EVENTBUS_INTEGRATION_REQUIRED") == "1" {
			t.Fatal("WS_NATS_INTEGRATION is required")
		}
		t.Skip("set WS_NATS_INTEGRATION=1")
	}
	cfg := commonnats.DefaultConfig()
	if url := os.Getenv("WS_NATS_URL"); url != "" {
		cfg.URLs = []string{url}
	} else if os.Getenv("EVENTBUS_INTEGRATION_REQUIRED") == "1" {
		t.Fatal("WS_NATS_URL is required")
	}
	client := commonnats.New(cfg)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	receiver := NewManagerWithClientAndPolicy(client, pubsub.StayUnready)
	sender := NewManagerWithClientAndPolicy(client, pubsub.StayUnready)
	t.Cleanup(func() { _ = receiver.Shutdown(); _ = sender.Shutdown() })
	done := make(chan struct{}, 1)
	receiver.RegisterHandler(pubsub.MessageTypeNotification, func(message *pubsub.CrossNodeMessage) {
		if message.UserID == "integration-user" {
			done <- struct{}{}
		}
	})
	if err := receiver.Subscribe("integration.nats"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := sender.Publish("integration.nats", &pubsub.CrossNodeMessage{Type: pubsub.MessageTypeNotification, UserID: "integration-user"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("message not delivered")
	}
}

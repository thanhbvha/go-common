//go:build integration

package natspubsub

import (
	"context"
	"os"
	"testing"
	"time"

	commonnats "github.com/thanhbvha/go-common/nats"
	"github.com/thanhbvha/go-common/websocket/pubsub"
)

func TestNATSIntegrationPublishSubscribe(t *testing.T) {
	if os.Getenv("WS_NATS_INTEGRATION") == "" {
		t.Skip("set WS_NATS_INTEGRATION=1")
	}
	client := commonnats.New(commonnats.DefaultConfig())
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

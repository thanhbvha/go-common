//go:build integration

package redispubsub

import (
	"context"
	"os"
	"testing"
	"time"

	commonredis "github.com/thanhbvha/go-common/redis"
	"github.com/thanhbvha/go-common/websocket/pubsub"
)

func TestRedisIntegrationPublishSubscribe(t *testing.T) {
	if os.Getenv("WS_REDIS_INTEGRATION") == "" {
		t.Skip("set WS_REDIS_INTEGRATION=1")
	}
	cfg := commonredis.DefaultConfig()
	client := commonredis.New(cfg)
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
	if err := receiver.Subscribe("integration:redis"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := sender.Publish("integration:redis", &pubsub.CrossNodeMessage{Type: pubsub.MessageTypeNotification, UserID: "integration-user"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("message not delivered")
	}
}

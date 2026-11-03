//go:build integration

package redispubsub

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	commonredis "github.com/thanhbvha/go-common/v2/redis"
	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
)

func TestRedisIntegrationPublishSubscribe(t *testing.T) {
	if os.Getenv("WS_REDIS_INTEGRATION") == "" {
		if os.Getenv("EVENTBUS_INTEGRATION_REQUIRED") == "1" {
			t.Fatal("WS_REDIS_INTEGRATION is required")
		}
		t.Skip("set WS_REDIS_INTEGRATION=1")
	}
	cfg := commonredis.DefaultConfig()
	if address := os.Getenv("WS_REDIS_ADDR"); address != "" {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			t.Fatal(err)
		}
		number, err := strconv.Atoi(port)
		if err != nil {
			t.Fatal(err)
		}
		if host == "" || number < 1 || number > 65535 {
			t.Fatal("WS_REDIS_ADDR must contain a host and a valid TCP port")
		}
		cfg.Host, cfg.Port = host, number
	} else if os.Getenv("EVENTBUS_INTEGRATION_REQUIRED") == "1" {
		t.Fatal("WS_REDIS_ADDR is required")
	}
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

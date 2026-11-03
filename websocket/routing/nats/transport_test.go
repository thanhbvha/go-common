package nats_test

import (
	"context"
	"errors"
	"github.com/alicebob/miniredis/v2"
	"github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
	"github.com/thanhbvha/go-common/v2/websocket/routing/contracttest"
	driver "github.com/thanhbvha/go-common/v2/websocket/routing/nats"
	registry "github.com/thanhbvha/go-common/v2/websocket/routing/redis"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCloseWaitsForInflightDeliveryHandler(t *testing.T) {
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: server.RANDOM_PORT, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(time.Second) {
		t.Fatal("NATS startup")
	}
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	nc, err := gonats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	receiver, sender := driver.NewTransport(nc, "close-test"), driver.NewTransport(nc, "close-test")
	entered, release := make(chan struct{}), make(chan struct{})
	var released sync.Once
	defer released.Do(func() { close(release) })
	var finished atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = receiver.Start(ctx, "receiver", func(context.Context, routing.DeliveryCommand) routing.DeliveryResult {
		close(entered)
		<-release
		finished.Store(true)
		return routing.DeliveryResult{}
	}); err != nil {
		t.Fatal(err)
	}
	if err = sender.Start(ctx, "sender", nil); err != nil {
		t.Fatal(err)
	}
	defer sender.Close(context.Background())
	sendCtx, stopSend := context.WithCancel(ctx)
	defer stopSend()
	sent := make(chan struct{})
	go func() {
		_, _ = sender.Deliver(sendCtx, "receiver", routing.DeliveryCommand{Version: 1, TargetNode: "receiver", TenantID: "t", MessageID: "m", ExpiresAt: time.Now().Add(time.Second)})
		close(sent)
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}
	closeCtx, stopClose := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopClose()
	if err = receiver.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close returned before callback: %v", err)
	}
	released.Do(func() { close(release) })
	if err = receiver.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !finished.Load() {
		t.Fatal("close did not join callback")
	}
	if !nc.IsConnected() {
		t.Fatal("transport closed borrowed broker client")
	}
	stopSend()
	<-sent
}

func TestContract(t *testing.T) {
	contracttest.Run(t, func(t *testing.T) (routing.SessionRegistry, func() routing.DeliveryTransport) {
		s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: server.RANDOM_PORT, NoLog: true, NoSigs: true})
		if err != nil {
			t.Fatal(err)
		}
		go s.Start()
		if !s.ReadyForConnections(5 * time.Second) {
			t.Fatal("NATS startup timeout")
		}
		t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
		nc, err := gonats.Connect(s.ClientURL())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		redis := miniredis.RunT(t)
		client := goredis.NewClient(&goredis.Options{Addr: redis.Addr()})
		t.Cleanup(func() { client.Close() })
		return registry.NewRegistry(client, "test"), func() routing.DeliveryTransport { return driver.NewTransport(nc, "test") }
	})
}

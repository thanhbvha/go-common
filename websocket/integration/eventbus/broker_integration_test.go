//go:build integration

package eventbus_test

import (
	"context"
	"io"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
	bus "github.com/thanhbvha/go-common/eventbus"
	natsbus "github.com/thanhbvha/go-common/eventbus/driver/nats"
	redisbus "github.com/thanhbvha/go-common/eventbus/driver/redis"
	bridge "github.com/thanhbvha/go-common/websocket/integration/eventbus"
	"github.com/thanhbvha/go-common/websocket/routing"
	"github.com/thanhbvha/go-common/websocket/routing/contracttest"
	natsrouting "github.com/thanhbvha/go-common/websocket/routing/nats"
	redisrouting "github.com/thanhbvha/go-common/websocket/routing/redis"
)

func realRedis(t *testing.T) *goredis.Client {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not installed")
	}
	socket := filepath.Join(t.TempDir(), "redis.sock")
	cmd := exec.Command(binary, "--port", "0", "--unixsocket", socket, "--save", "", "--appendonly", "no")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	client := goredis.NewClient(&goredis.Options{Network: "unix", Addr: socket})
	t.Cleanup(func() { client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for client.Ping(ctx).Err() != nil {
		if ctx.Err() != nil {
			t.Fatal("Redis startup timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return client
}
func TestRealRedisRoutingContract(t *testing.T) {
	client := realRedis(t)
	contracttest.Run(t, func(t *testing.T) (routing.SessionRegistry, func() routing.DeliveryTransport) {
		return redisrouting.NewRegistry(client, "contract"), func() routing.DeliveryTransport { return redisrouting.NewTransport(client, "contract") }
	})
}
func TestRealRedisStreamsBridge(t *testing.T) {
	client := realRedis(t)
	runBridge(t, func() (bus.Bus, error) { return bus.Open(bus.DefaultConfig(), bus.WithTransport(redisbus.New(client))) }, []bus.EventOption{redisbus.WithEventLayoutTargeted(), redisbus.WithBatchSize(64), redisbus.WithBlock(25 * time.Millisecond)}, client)
	competingRouters(t, func() (bus.Bus, error) { return bus.Open(bus.DefaultConfig(), bus.WithTransport(redisbus.New(client))) }, []bus.EventOption{redisbus.WithEventLayoutTargeted(), redisbus.WithBatchSize(64), redisbus.WithBlock(25 * time.Millisecond)})
}
func TestRealNATSJetStreamBridge(t *testing.T) {
	client := realRedis(t)
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: server.RANDOM_PORT, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS startup timeout")
	}
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	nc, err := gonats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	runBridge(t, func() (bus.Bus, error) { return bus.Open(bus.DefaultConfig(), bus.WithTransport(natsbus.New(js))) }, []bus.EventOption{natsbus.WithEventLayoutTargeted(), natsbus.WithBatchSize(64), natsbus.WithFetchTimeout(25 * time.Millisecond)}, client, func() routing.DeliveryTransport { return natsrouting.NewTransport(nc, "bridge") })
	competingRouters(t, func() (bus.Bus, error) { return bus.Open(bus.DefaultConfig(), bus.WithTransport(natsbus.New(js))) }, []bus.EventOption{natsbus.WithEventLayoutTargeted(), natsbus.WithBatchSize(64), natsbus.WithFetchTimeout(25 * time.Millisecond)})
}

func competingRouters(t *testing.T, openBus func() (bus.Bus, error), options []bus.EventOption) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	target := &resultTransport{}
	router, _ := routing.NewRouter(routing.RouterConfig{Registry: staticRegistry{location: routing.Location{TenantID: "t", UserID: "u", SessionID: "s", NodeID: "n", Generation: "g"}}, Transport: target})
	var publisher *bridge.Bridge
	var buses []bus.Bus
	for i := 0; i < 2; i++ {
		b, err := openBus()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { b.Close(context.Background()) })
		buses = append(buses, b)
		integration, err := bridge.New(bridge.Config{Bus: b, Router: router, NotificationEvent: "competing.notification", NotificationOptions: options})
		if err != nil {
			t.Fatal(err)
		}
		if err = b.Start(ctx); err != nil {
			t.Fatal(err)
		}
		publisher = integration
	}
	for i := 0; i < 25; i++ {
		if err := publisher.PublishNotification(ctx, bridge.Notification{MessageID: routing.NewID(), TenantID: "t", UserID: "u", Payload: []byte("message")}); err != nil {
			t.Fatal(err)
		}
	}
	for target.attempts.Load() < 25 {
		select {
		case <-ctx.Done():
			for _, b := range buses {
				stats, err := b.SubscriberStats(context.Background(), "competing.notification", "realtime-router")
				t.Logf("stats=%+v err=%v", stats, err)
			}
			t.Fatalf("router group consumed %d/25 notifications", target.attempts.Load())
		case <-time.After(time.Millisecond):
		}
	}
	for _, b := range buses {
		if err := b.Drain(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Inspect settled deliveries after both consumers drained.
	if target.attempts.Load() != 25 {
		t.Fatalf("each event reached multiple router instances: %d", target.attempts.Load())
	}
}

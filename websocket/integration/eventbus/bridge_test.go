package eventbus_test

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	bus "github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/driver/memory"
	bridge "github.com/thanhbvha/go-common/websocket/integration/eventbus"
	"github.com/thanhbvha/go-common/websocket/routing"
	driver "github.com/thanhbvha/go-common/websocket/routing/redis"
	"testing"
	"time"
)

func TestBridgeBorrowedBusAndDurableCommand(t *testing.T) {
	runBridge(t, func() (bus.Bus, error) { return bus.Open(bus.DefaultConfig(), bus.WithTransport(memory.New())) }, nil, nil)
}
func runBridge(t *testing.T, openBus func() (bus.Bus, error), options []bus.EventOption, providedClient *goredis.Client, transports ...func() routing.DeliveryTransport) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := providedClient
	if client == nil {
		server := miniredis.RunT(t)
		client = goredis.NewClient(&goredis.Options{Addr: server.Addr()})
		defer client.Close()
	}
	registry := driver.NewRegistry(client, "bridge")
	newTransport := func() routing.DeliveryTransport { return driver.NewTransport(client, "bridge") }
	if len(transports) > 0 {
		newTransport = transports[0]
	}
	gateway, err := routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: newTransport()})
	if err != nil {
		t.Fatal(err)
	}
	if err = gateway.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer gateway.Close(ctx)
	frames := make(chan []byte, 2)
	_, err = gateway.Register(ctx, routing.Location{TenantID: "tenant", UserID: "user", SessionID: "session"}, func(b []byte) error { frames <- b; return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	sender := newTransport()
	if err = sender.Start(ctx, routing.NewID(), nil); err != nil {
		t.Fatal(err)
	}
	defer sender.Close(ctx)
	router, _ := routing.NewRouter(routing.RouterConfig{Registry: registry, Transport: sender})
	b, err := openBus()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(context.Background())
	integration, err := bridge.New(bridge.Config{Bus: b, Router: router, NotificationOptions: options})
	if err != nil {
		t.Fatal(err)
	}
	commands := make(chan routing.BusinessCommand, 1)
	release := make(chan struct{})
	if err = b.HandleTask("ws.command", func(ctx context.Context, j bus.Job) error {
		var c routing.BusinessCommand
		if err := j.Decode(&c); err != nil {
			return err
		}
		commands <- c
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return integration.PublishNotification(ctx, bridge.Notification{MessageID: j.ID() + "-result", TenantID: c.TenantID, UserID: c.UserID, Payload: []byte("completed")})
	}); err != nil {
		t.Fatal(err)
	}
	if err = b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	connectionCtx, disconnect := context.WithCancel(ctx)
	if err = integration.PublishCommand(connectionCtx, routing.BusinessCommand{MessageID: "command", Type: "business", TenantID: "tenant", UserID: "user", SessionID: "session", Payload: []byte(`{"value":1}`)}); err != nil {
		t.Fatal(err)
	}
	disconnect()
	select {
	case c := <-commands:
		if c.TenantID != "tenant" {
			t.Fatal("identity lost")
		}
	case <-ctx.Done():
		t.Fatal("command not consumed")
	}
	close(release)
	select {
	case b := <-frames:
		if string(b) != "completed" {
			t.Fatal("payload")
		}
	case <-ctx.Done():
		t.Fatal("accepted command canceled by disconnect")
	}
	if err = b.Ping(ctx); err != nil {
		t.Fatalf("borrowed bus unusable: %v", err)
	}
	if _, err = bridge.New(bridge.Config{Bus: b, Router: router, NotificationOptions: options}); err == nil {
		t.Fatal("late declarations should fail")
	}
}

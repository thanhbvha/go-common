package core

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/websocket/routing"
	driver "github.com/thanhbvha/go-common/websocket/routing/redis"
	"testing"
	"time"
)

func TestDistributedManagerRoomAndSessionLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	registry := driver.NewRegistry(client, "manager")
	var gateways []*routing.Gateway
	var managers []*Manager
	for i := 0; i < 2; i++ {
		g, err := routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: driver.NewTransport(client, "manager")})
		if err != nil {
			t.Fatal(err)
		}
		if err = g.Start(ctx); err != nil {
			t.Fatal(err)
		}
		gateways = append(gateways, g)
		c := DefaultManagerConfig()
		c.Distributed = g
		managers = append(managers, NewManagerWithConfig(c))
	}
	defer func() {
		for _, m := range managers {
			if err := m.Shutdown(ctx); err != nil {
				t.Error(err)
			}
		}
		for _, g := range gateways {
			if err := g.Close(ctx); err != nil {
				t.Error(err)
			}
		}
	}()
	transports := []*mockConn{newMockConn(), newMockConn(), newMockConn()}
	done := make(chan error, 3)
	for i, tr := range transports {
		m := managers[i%2]
		user := string(rune('a' + i))
		shard := "shard-" + user
		go func() {
			done <- m.HandleIdentityConnection(tr, shard, Identity{TenantID: "tenant", UserID: user, SessionID: "session"}, "", "")
		}()
	}
	waitForCondition(t, func() bool { return managers[0].GetTotalConnections() == 2 && managers[1].GetTotalConnections() == 1 })
	for _, m := range managers {
		m.shardMutex.RLock()
		for _, s := range m.shards {
			s.mu.RLock()
			var cs []*Connection
			for c := range s.activeConns {
				cs = append(cs, c)
			}
			s.mu.RUnlock()
			for _, c := range cs {
				if err := s.JoinRoom(ctx, "room", c); err != nil {
					t.Fatal(err)
				}
			}
		}
		m.shardMutex.RUnlock()
	}
	sender := driver.NewTransport(client, "manager")
	if err := sender.Start(ctx, routing.NewID(), nil); err != nil {
		t.Fatal(err)
	}
	defer sender.Close(ctx)
	router, _ := routing.NewRouter(routing.RouterConfig{Registry: registry, Transport: sender})
	result, err := router.SendToRoom(ctx, "tenant", "room", []byte("hello"), "room-message")
	if err != nil || result.Retryable() || result.Count(routing.Enqueued) != 3 {
		t.Fatalf("room: %+v %v", result, err)
	}
	for _, tr := range transports {
		select {
		case frame := <-tr.writes:
			if string(frame.data) != "hello" {
				t.Fatal("wrong payload")
			}
		case <-ctx.Done():
			t.Fatal("missing frame")
		}
	}
	for _, tr := range transports {
		tr.Close()
	}
	for range transports {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	nodes, err := registry.ResolveRoom(ctx, "tenant", "room")
	if err != nil || len(nodes) != 0 {
		t.Fatalf("room cleanup: %v %v", nodes, err)
	}
}

type rejectingRegistration struct{ routing.SessionRegistry }

func (r rejectingRegistration) Register(context.Context, routing.Location, time.Duration) (routing.Location, error) {
	return routing.Location{}, routing.ErrUnavailable
}
func TestDistributedRegistrationFailureRollsBackQuota(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	gateway, _ := routing.NewGateway(routing.GatewayConfig{Registry: rejectingRegistration{driver.NewRegistry(client, "reject")}, Transport: driver.NewTransport(client, "reject")})
	if err := gateway.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer gateway.Close(ctx)
	config := DefaultManagerConfig()
	config.Distributed = gateway
	config.MaxConnectionsPerUser = 1
	manager := NewManagerWithConfig(config)
	defer manager.Shutdown(ctx)
	tr := newMockConn()
	err := manager.HandleIdentityConnection(tr, "", Identity{TenantID: "t", UserID: "u", SessionID: "s"}, "", "")
	if err == nil {
		t.Fatal("registration succeeded")
	}
	if manager.GetTotalConnections() != 0 || manager.reservedConnections != 0 || len(manager.identityUsage) != 0 || gateway.Stats().Sessions != 0 || tr.closeCount.Load() != 1 {
		t.Fatalf("failed registration leaked quota/session: %v", manager.GetStats())
	}
}

// Exercise the lower-level shard API without a Manager handler/deferred Remove.
func TestDistributedShardShutdownWithoutRunningHandler(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "cleanup"
		if canceled {
			name = "expired_cleanup_budget"
		}
		t.Run(name, func(t *testing.T) { testDistributedShardShutdown(t, canceled) })
	}
}
func testDistributedShardShutdown(t *testing.T, canceled bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	registry := driver.NewRegistry(client, "shard-cleanup")
	g, _ := routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: driver.NewTransport(client, "shard-cleanup")})
	if err := g.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer g.Close(ctx)
	config := DefaultManagerConfig()
	config.Distributed = g
	config.DisableLifecycleLogging = true
	manager := NewManagerWithConfig(config)
	defer manager.Shutdown(ctx)
	shard := manager.GetOrCreateShard("direct")
	conn := NewConnection(newMockConn(), shard, "u", shard.name, "", g.NodeID(), "")
	conn.setIdentity(Identity{TenantID: "t", UserID: "u", SessionID: "s"})
	l, err := g.Register(ctx, routing.Location{TenantID: "t", UserID: "u", SessionID: "s"}, func([]byte) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	conn.location = &l
	if !shard.Register(conn) {
		t.Fatal("shard registration rejected")
	}
	if err = shard.JoinRoom(ctx, "r", conn); err != nil {
		t.Fatal(err)
	}
	if canceled {
		cleanupCtx, stop := context.WithCancel(ctx)
		stop()
		shard.shutdown(cleanupCtx)
	} else if err = shard.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if !conn.IsClosed() || shard.GetConnectionCount() != 0 || g.Stats().Sessions != 0 {
		t.Fatal("local session leaked")
	}
	locations, err := registry.Resolve(ctx, "t", "u", "s")
	expected := 0
	if canceled {
		expected = 1
	}
	if err != nil || len(locations) != expected {
		t.Fatalf("registry session leaked: %v %v", locations, err)
	}
	if canceled && g.Stats().UnregisterFailures == 0 {
		t.Fatal("canceled cleanup was not counted")
	}
	nodes, err := registry.ResolveRoom(ctx, "t", "r")
	if err != nil || len(nodes) != expected {
		t.Fatalf("registry room leaked: %v %v", nodes, err)
	}
}

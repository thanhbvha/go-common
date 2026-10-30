package core

import (
	"context"
	"errors"
	"github.com/thanhbvha/go-common/websocket/routing"
	"testing"
	"time"
)

func TestBroadcastOwnsPayload(t *testing.T) {
	coordinator := &lifecycleCoordinator{}
	shard := NewShard("broadcast", coordinator)
	conn := NewConnection(newMockConn(), shard, "u", "broadcast", "", "", "")
	if !shard.Register(conn) {
		t.Fatal("register")
	}
	payload := []byte("original")
	shard.Broadcast(payload)
	payload[0] = 'X'
	if got := string(<-conn.send); got != "original" {
		t.Fatalf("payload mutated: %s", got)
	}
	shard.Unregister(conn)
	coordinator.GetEventExecutor().Shutdown(context.Background())
}
func TestQueueRejectionDoesNotDisconnectEarly(t *testing.T) {
	config := DefaultManagerConfig()
	config.SendQueueSize = 1
	config.SlowConsumerDropThreshold = 3
	m := NewManagerWithConfig(config)
	defer m.Shutdown(context.Background())
	s, _ := m.GetShard(defaultShardName)
	c := NewConnection(newMockConn(), s, "u", s.name, "", "", "")
	s.Register(c)
	if err := c.Enqueue(nil); err != nil {
		t.Fatal(err)
	}
	s.SendToUser("u", nil)
	if c.IsClosed() || s.GetConnectionCount() != 1 {
		t.Fatal("queue rejection disconnected before threshold")
	}
	if c.Send(nil) {
		t.Fatal("Send returned true without enqueue")
	}
	if c.IsClosed() {
		t.Fatal("second rejection closed before threshold")
	}
	if err := c.Enqueue(nil); !errors.Is(err, ErrSendQueueFull) || !c.IsClosed() {
		t.Fatalf("third rejection: %v", err)
	}
	s.Unregister(c)
}
func TestShardCapacityDoesNotReroute(t *testing.T) {
	config := DefaultManagerConfig()
	config.MaxShards = 2
	m := NewManagerWithConfig(config)
	defer m.Shutdown(context.Background())
	s := m.AddShard("one")
	if s == nil {
		t.Fatal("first shard rejected")
	}
	if m.AddShard("one") != s {
		t.Fatal("existing shard unavailable at capacity")
	}
	if m.AddShard("two") != nil {
		t.Fatal("new shard silently rerouted")
	}
}

type commandCapture struct{ commands chan routing.BusinessCommand }

func (p commandCapture) PublishCommand(ctx context.Context, c routing.BusinessCommand) error {
	p.commands <- c
	return nil
}
func TestBusinessDispatchUsesAuthenticatedIdentity(t *testing.T) {
	registry := NewEventRegistry()
	registry.Register("business", HandlerConfig{Dispatch: Business})
	capture := commandCapture{commands: make(chan routing.BusinessCommand, 1)}
	config := DefaultManagerConfig()
	config.EventRegistry = registry
	config.CommandPublisher = capture
	m := NewManagerWithConfig(config)
	defer m.Shutdown(context.Background())
	conn := newMockConn()
	done := make(chan error, 1)
	go func() {
		done <- m.HandleIdentityConnection(conn, "", Identity{TenantID: "trusted-tenant", UserID: "trusted-user", AccountID: "trusted-account"}, "", "request")
	}()
	conn.reads <- []byte(`{"type":"business","data":{"tenant_id":"forged","user_id":"forged"}}`)
	select {
	case c := <-capture.commands:
		if c.TenantID != "trusted-tenant" || c.UserID != "trusted-user" || c.AccountID != "trusted-account" || c.SessionID == "" {
			t.Fatalf("identity forged: %+v", c)
		}
	case <-time.After(time.Second):
		t.Fatal("business command not dispatched")
	}
	conn.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type blockedClose struct {
	benchmarkConn
	gate chan struct{}
}

func (c blockedClose) Close() error { <-c.gate; return nil }
func TestShardShutdownHonorsDeadlineWithBlockedClose(t *testing.T) {
	coordinator := &lifecycleCoordinator{}
	defer coordinator.GetEventExecutor().Shutdown(context.Background())
	shard := NewShard("blocked-close", coordinator)
	gate := make(chan struct{})
	c := NewConnection(blockedClose{gate: gate}, shard, "user", shard.name, "", "", "")
	shard.Register(c)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := shard.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown blocked or ignored deadline: %v", err)
	}
	close(gate)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := shard.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

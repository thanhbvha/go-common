package core

import (
	"context"
	"errors"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
	"sync"
	"testing"
	"time"
)

type roomIORegistry struct {
	routing.SessionRegistry
	entered chan struct{}
	release chan struct{}
	fail    bool
	once    sync.Once
}

func (r *roomIORegistry) Renew(context.Context, []routing.Location, time.Duration) error { return nil }
func (r *roomIORegistry) Register(_ context.Context, l routing.Location, _ time.Duration) (routing.Location, error) {
	return l, nil
}
func (r *roomIORegistry) Remove(context.Context, routing.Location) error            { return nil }
func (r *roomIORegistry) LeaveRoom(context.Context, routing.Location, string) error { return nil }
func (r *roomIORegistry) JoinRoom(ctx context.Context, _ routing.Location, _ string, _ time.Duration) error {
	r.once.Do(func() { close(r.entered) })
	select {
	case <-r.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if r.fail {
		return routing.ErrUnavailable
	}
	return nil
}

type roomIOTransport struct{ routing.DeliveryTransport }

func (roomIOTransport) Start(context.Context, string, routing.DeliveryHandler) error { return nil }
func (roomIOTransport) Ready() bool                                                  { return true }
func (roomIOTransport) Close(context.Context) error                                  { return nil }
func TestRoomRegistryIODoesNotHoldShardLock(t *testing.T) {
	for _, fail := range []bool{false, true} {
		for _, closedDuringJoin := range []bool{false, true} {
			t.Run(map[bool]string{false: "success", true: "rollback"}[fail]+map[bool]string{false: "", true: "_closed"}[closedDuringJoin], func(t *testing.T) {
				registry := &roomIORegistry{entered: make(chan struct{}), release: make(chan struct{}), fail: fail}
				gateway, err := routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: roomIOTransport{}, RenewInterval: time.Hour, LeaseTTL: 2 * time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				if err = gateway.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				defer gateway.Close(context.Background())
				coordinator := &lifecycleCoordinator{}
				shard := NewShard("room-io", coordinator)
				shard.config.Distributed = gateway
				conn := NewConnection(newMockConn(), shard, "u", "room-io", "", "", "")
				conn.setIdentity(Identity{TenantID: "t", UserID: "u", SessionID: "s"})
				loc, err := gateway.Register(context.Background(), routing.Location{TenantID: "t", UserID: "u", SessionID: "s"}, func([]byte) error { return nil }, nil)
				if err != nil {
					t.Fatal(err)
				}
				conn.location = &loc
				if !shard.Register(conn) {
					t.Fatal("register failed")
				}
				joined := make(chan error, 1)
				go func() { joined <- shard.JoinRoom(context.Background(), "room", conn) }()
				<-registry.entered
				read := make(chan struct{})
				go func() { shard.GetRoomSize("unrelated"); close(read) }()
				select {
				case <-read:
				case <-time.After(150 * time.Millisecond):
					close(registry.release)
					<-joined
					t.Fatal("registry I/O held shard lock")
				}
				if closedDuringJoin {
					conn.Close("closed during registry I/O")
				}
				close(registry.release)
				err = <-joined
				if fail && !errors.Is(err, routing.ErrUnavailable) {
					t.Fatalf("join error=%v", err)
				}
				want := 1
				if !fail && closedDuringJoin && !errors.Is(err, ErrConnectionInactive) {
					t.Fatalf("late join accepted: %v", err)
				}
				if fail || closedDuringJoin {
					want = 0
				}
				if got := shard.GetRoomSize("room"); got != want {
					t.Fatalf("room size=%d want=%d", got, want)
				}
				shard.Unregister(conn)
			})
		}

	}
}

func TestUnregisterDuringRoomJoinCannotRestoreMembership(t *testing.T) {
	registry := &roomIORegistry{entered: make(chan struct{}), release: make(chan struct{})}
	gateway, err := routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: roomIOTransport{}, LeaseTTL: 2 * time.Hour, RenewInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err = gateway.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer gateway.Close(context.Background())
	shard := NewShard("unregister-join", &lifecycleCoordinator{})
	shard.config.Distributed = gateway
	conn := NewConnection(newMockConn(), shard, "u", shard.name, "", "", "")
	conn.setIdentity(Identity{TenantID: "t", UserID: "u", SessionID: "s"})
	location, err := gateway.Register(context.Background(), routing.Location{TenantID: "t", UserID: "u", SessionID: "s"}, func([]byte) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	conn.location = &location
	if !shard.Register(conn) {
		t.Fatal("register failed")
	}
	join := make(chan error, 1)
	go func() { join <- shard.JoinRoom(context.Background(), "room", conn) }()
	<-registry.entered
	removed := make(chan struct{})
	go func() { shard.Unregister(conn); close(removed) }()
	// Removal closes the connection after detaching local indexes, before the
	// remote Remove can acquire the gateway's in-flight registry operation lock.
	deadline := time.After(time.Second)
	for !conn.IsClosed() {
		select {
		case <-deadline:
			close(registry.release)
			t.Fatal("local unregister blocked on registry")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if shard.GetRoomSize("room") != 0 || shard.GetConnectionCount() != 0 {
		close(registry.release)
		t.Fatal("local removal incomplete")
	}
	close(registry.release)
	select {
	case err := <-join:
		if !errors.Is(err, ErrConnectionInactive) {
			t.Fatalf("late join=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("late join blocked")
	}
	select {
	case <-removed:
	case <-time.After(time.Second):
		t.Fatal("unregister blocked")
	}
	if shard.GetRoomSize("room") != 0 || gateway.Stats().Sessions != 0 {
		t.Fatal("late room join restored removed session")
	}
}

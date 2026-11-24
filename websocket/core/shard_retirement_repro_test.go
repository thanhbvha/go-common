package core

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/websocket/routing"
)

type gatedShardRegistration struct {
	roomIORegistry
	once sync.Once
}

func (r *gatedShardRegistration) Register(ctx context.Context, l routing.Location, _ time.Duration) (routing.Location, error) {
	r.once.Do(func() { close(r.entered) })
	select {
	case <-r.release:
		return l, nil
	case <-ctx.Done():
		return routing.Location{}, ctx.Err()
	}
}

type disconnectAfterAdmission struct{ *mockConn }

func (disconnectAfterAdmission) ReadMessage() (int, []byte, error) { return 0, nil, io.EOF }

func TestShardCleanupDoesNotRejectInFlightAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	registry := &gatedShardRegistration{roomIORegistry: roomIORegistry{
		entered: make(chan struct{}), release: make(chan struct{}),
	}}
	gateway, err := routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: roomIOTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := gateway.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gateway.Close(context.Background()) })
	manager := NewManagerWithConfig(ManagerConfig{Distributed: gateway, DisableLifecycleLogging: true})
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	const shardID = "idle"
	if manager.GetOrCreateShard(shardID) == nil {
		t.Fatal("could not create idle shard")
	}
	done := make(chan error, 1)
	go func() {
		done <- manager.HandleIdentityConnection(disconnectAfterAdmission{newMockConn()}, shardID,
			Identity{TenantID: "tenant", UserID: "user", SessionID: "session"}, "", "")
	}()
	// The handler has selected its shard but has not registered the connection in it yet.
	select {
	case <-registry.entered:
	case err := <-done:
		t.Fatalf("admission ended before registry gate: %v", err)
	case <-ctx.Done():
		t.Fatal("admission did not reach registry gate")
	}
	manager.cleanupEmptyShards()
	close(registry.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("idle shard cleanup rejected in-flight admission: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("admission did not finish after registry gate was released")
	}
}

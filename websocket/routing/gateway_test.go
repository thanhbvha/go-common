package routing_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/websocket/routing"
	driver "github.com/thanhbvha/go-common/websocket/routing/redis"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type faultyRegistry struct {
	routing.SessionRegistry
	failed atomic.Bool
}

func (r *faultyRegistry) Renew(ctx context.Context, ls []routing.Location, ttl time.Duration) error {
	if r.failed.Load() {
		return routing.ErrUnavailable
	}
	return r.SessionRegistry.Renew(ctx, ls, ttl)
}
func TestGatewayLeaseLossRecoveryAndFencing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	registry := &faultyRegistry{SessionRegistry: driver.NewRegistry(client, "failure")}
	g, err := routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: driver.NewTransport(client, "failure"), LeaseTTL: 120 * time.Millisecond, RenewInterval: 20 * time.Millisecond, DedupeCapacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err = g.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer g.Close(ctx)
	var enqueued atomic.Int64
	var closed atomic.Bool
	l, err := g.Register(ctx, routing.Location{TenantID: "t", UserID: "u", SessionID: "s"}, func([]byte) error { enqueued.Add(1); return nil }, func() { closed.Store(true) })
	if err != nil {
		t.Fatal(err)
	}
	registry.failed.Store(true)
	wait(t, func() bool { return !g.Ready() })
	if _, err = g.Register(ctx, routing.Location{TenantID: "t", UserID: "u"}, func([]byte) error { return nil }, nil); !errors.Is(err, routing.ErrUnavailable) {
		t.Fatalf("accepted with expired lease: %v", err)
	}
	registry.failed.Store(false)
	wait(t, g.Ready)
	for i := 0; i < 3; i++ {
		res := g.Handle(ctx, routing.DeliveryCommand{Version: 1, MessageID: routing.NewID(), TenantID: "t", TargetNode: g.NodeID(), Sessions: []routing.Location{l}, ExpiresAt: time.Now().Add(time.Second)})
		if res.Retryable() {
			t.Fatalf("recovered delivery: %+v", res)
		}
	}
	if g.Stats().DedupeEntries > 2 {
		t.Fatal("dedupe cache unbounded")
	}
	l.Generation = "new-owner"
	l.NodeID = "other-node"
	if _, err = registry.Register(ctx, l, time.Second); err != nil {
		t.Fatal(err)
	}
	wait(t, closed.Load)
	if g.Stats().Sessions != 0 {
		t.Fatal("superseded session still active")
	}
}
func TestGatewayCloseBeforeStart(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	g, _ := routing.NewGateway(routing.GatewayConfig{Registry: driver.NewRegistry(client, "close"), Transport: driver.NewTransport(client, "close")})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Start(ctx); err == nil {
		t.Fatal("started a closed gateway")
	}
}
func wait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition timeout")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestGatewayClonesOnceForImmutableFanOut(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	g, _ := routing.NewGateway(routing.GatewayConfig{Registry: driver.NewRegistry(client, "payload"), Transport: driver.NewTransport(client, "payload")})
	if err := g.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer g.Close(ctx)
	var frames [][]byte
	var locations []routing.Location
	for _, session := range []string{"one", "two"} {
		l, err := g.Register(ctx, routing.Location{TenantID: "t", UserID: "u", SessionID: session}, func(b []byte) error { frames = append(frames, b); return nil }, nil)
		if err != nil {
			t.Fatal(err)
		}
		locations = append(locations, l)
	}
	payload := []byte("original")
	res := g.Handle(ctx, routing.DeliveryCommand{Version: 1, TenantID: "t", MessageID: "payload", TargetNode: g.NodeID(), Sessions: locations, Payload: payload, ExpiresAt: time.Now().Add(time.Second)})
	payload[0] = 'X'
	if res.Count(routing.Enqueued) != 2 || len(frames) != 2 || string(frames[0]) != "original" || string(frames[1]) != "original" {
		t.Fatalf("payload ownership failed: %+v %q", res, frames)
	}
	if &frames[0][0] != &frames[1][0] {
		t.Fatal("fan-out copied per recipient")
	}
}

// This wrapper deliberately exposes only the original registry contract.
type cleanupRegistry struct {
	routing.SessionRegistry
	reject     atomic.Bool
	removes    atomic.Int64
	leaves     atomic.Int64
	alwaysFail bool
}

func (r *cleanupRegistry) Register(ctx context.Context, l routing.Location, ttl time.Duration) (routing.Location, error) {
	if r.reject.Load() {
		return routing.Location{}, routing.ErrUnavailable
	}
	return r.SessionRegistry.Register(ctx, l, ttl)
}
func (r *cleanupRegistry) Remove(ctx context.Context, l routing.Location) error {
	n := r.removes.Add(1)
	if n < 3 || r.alwaysFail {
		return routing.ErrUnavailable
	}
	return r.SessionRegistry.Remove(ctx, l)
}
func (r *cleanupRegistry) LeaveRoom(ctx context.Context, l routing.Location, room string) error {
	r.leaves.Add(1)
	return r.SessionRegistry.LeaveRoom(ctx, l, room)
}
func TestGatewayRollbackAndUnregisterRetry(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	base := driver.NewRegistry(client, "rollback")
	registry := &cleanupRegistry{SessionRegistry: base}
	ctx := context.Background()
	g, _ := routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: driver.NewTransport(client, "rollback")})
	if err := g.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer g.Close(ctx)
	var closed atomic.Int64
	l, err := g.Register(ctx, routing.Location{TenantID: "t", UserID: "u", SessionID: "s"}, func([]byte) error { return nil }, func() { closed.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	if err = g.JoinRoom(ctx, l, "r"); err != nil {
		t.Fatal(err)
	}
	registry.reject.Store(true)
	if _, err = g.Register(ctx, l, func([]byte) error { return nil }, nil); err == nil {
		t.Fatal("replacement succeeded")
	}
	result := g.Handle(ctx, routing.DeliveryCommand{Version: 1, MessageID: "m", TenantID: "t", TargetNode: g.NodeID(), RoomID: "r", ExpiresAt: time.Now().Add(time.Second)})
	if closed.Load() != 0 || result.Count(routing.Enqueued) != 1 {
		t.Fatalf("rollback lost old session/room: %+v", result)
	}
	if err = g.Remove(ctx, l); err != nil {
		t.Fatal(err)
	}
	stats := g.Stats()
	if stats.Sessions != 0 || stats.Rooms != 0 || stats.UnregisterFailures != 2 || stats.UnregisterRetries != 2 || registry.leaves.Load() != 3 {
		t.Fatalf("cleanup stats: %+v", stats)
	}
	locations, err := base.Resolve(ctx, "t", "u", "")
	if err != nil || len(locations) != 0 {
		t.Fatalf("session leaked: %v %v", locations, err)
	}
	registry.alwaysFail = true
	if err = g.Remove(ctx, l); !errors.Is(err, routing.ErrUnavailable) {
		t.Fatalf("missing final failure: %v", err)
	}
	if registry.removes.Load() != 6 {
		t.Fatal("unbounded retries")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err = g.Remove(canceled, l); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if registry.removes.Load() != 6 {
		t.Fatal("called registry after cancellation")
	}
}
func TestGatewayConcurrentLifecycleAndRenewal(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	registry := driver.NewRegistry(client, "concurrent")
	ctx := context.Background()
	g, _ := routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: driver.NewTransport(client, "concurrent"), LeaseTTL: time.Minute, RenewInterval: time.Millisecond * 10})
	if err := g.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer g.Close(ctx)
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Go(func() {
			for j := 0; j < 10; j++ {
				l, err := g.Register(ctx, routing.Location{TenantID: "t", UserID: fmt.Sprint(i), SessionID: fmt.Sprint(j)}, func([]byte) error { return nil }, nil)
				if err != nil {
					t.Error(err)
					return
				}
				if err = g.JoinRoom(ctx, l, "r"); err != nil {
					t.Error(err)
				}
				if err = g.LeaveRoom(ctx, l, "r"); err != nil {
					t.Error(err)
				}
				if err = g.Remove(ctx, l); err != nil {
					t.Error(err)
				}
			}
		})
	}
	workers.Wait()
	if stats := g.Stats(); stats.Sessions != 0 || stats.Rooms != 0 {
		t.Fatalf("leaked indexes: %+v", stats)
	}
}

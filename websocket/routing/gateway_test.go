package routing_test

import (
	"context"
	"errors"
	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/websocket/routing"
	driver "github.com/thanhbvha/go-common/websocket/routing/redis"
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

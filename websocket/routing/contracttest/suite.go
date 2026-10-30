// Package contracttest contains the common registry/transport behavior suite.
package contracttest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/websocket/routing"
)

type Factory func(*testing.T) (routing.SessionRegistry, func() routing.DeliveryTransport)

func Run(t *testing.T, f Factory) {
	t.Helper()
	registry, newTransport := f(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	gateway := func() *routing.Gateway {
		g, err := routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: newTransport()})
		if err != nil {
			t.Fatal(err)
		}
		if err = g.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			c, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := g.Close(c); err != nil {
				t.Error(err)
			}
		})
		return g
	}
	a, b := gateway(), gateway()
	unused := gateway()
	sender := newTransport()
	if err := sender.Start(ctx, routing.NewID(), nil); err != nil {
		t.Fatal(err)
	}
	defer sender.Close(ctx)
	router, err := routing.NewRouter(routing.RouterConfig{Registry: registry, Transport: sender, Timeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var counts [4]atomic.Int64
	var full atomic.Bool
	register := func(g *routing.Gateway, tenant, user, session, shard string, index int) routing.Location {
		l, err := g.Register(ctx, routing.Location{TenantID: tenant, UserID: user, SessionID: session, ShardID: shard}, func([]byte) error {
			if index == 0 && full.Load() {
				return routing.ErrQueueFull
			}
			counts[index].Add(1)
			return nil
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	first := register(a, "tenant", "user", "one", "shard-a", 0)
	second := register(b, "tenant", "user", "two", "shard-b", 1)
	third := register(a, "tenant", "other", "three", "shard-c", 2)
	foreign := register(b, "other-tenant", "user", "one", "shard-a", 3)
	for _, l := range []routing.Location{first, third} {
		if err := a.JoinRoom(ctx, l, "room"); err != nil {
			t.Fatal(err)
		}
	}
	for _, l := range []routing.Location{second, foreign} {
		if err := b.JoinRoom(ctx, l, "room"); err != nil {
			t.Fatal(err)
		}
	}
	res, err := router.SendToUser(ctx, "tenant", "user", []byte("hello"), "user-message")
	if err != nil || len(res.Outcomes) != 2 || res.Retryable() {
		t.Fatalf("user result=%+v error=%v", res, err)
	}
	if router.Stats().Attempts != 2 || unused.Stats().Enqueued != 0 || unused.Stats().Rejected != 0 {
		t.Fatal("user delivery fanned out to unrelated node")
	}
	if counts[0].Load() != 1 || counts[1].Load() != 1 || counts[2].Load() != 0 || counts[3].Load() != 0 {
		t.Fatal("user routing leaked or missed a session")
	}
	res, err = router.SendToRoom(ctx, "tenant", "room", []byte("room"), "room-message")
	if err != nil || res.Count(routing.Enqueued) != 3 || res.Retryable() {
		t.Fatalf("room result=%+v error=%v", res, err)
	}
	_, err = router.SendToRoom(ctx, "tenant", "room", []byte("room"), "room-message")
	if err != nil {
		t.Fatal(err)
	}
	if counts[0].Load() != 2 || counts[1].Load() != 2 || counts[2].Load() != 1 || counts[3].Load() != 0 {
		t.Fatal("room cross-shard, tenant isolation, or dedupe failed")
	}
	full.Store(true)
	res, err = router.SendToUser(ctx, "tenant", "user", nil, "partial")
	if err != nil || !res.Retryable() {
		t.Fatalf("expected partial queue rejection: %+v %v", res, err)
	}
	full.Store(false)
	res, err = router.SendToSession(ctx, "tenant", "user", "one", nil, "partial")
	if err != nil || res.Retryable() || counts[0].Load() != 3 {
		t.Fatalf("rejected enqueue was deduped: %+v %v", res, err)
	}
	replacement := register(b, "tenant", "user", "one", "shard-d", 0)
	if err := a.Remove(ctx, first); err != nil {
		t.Fatal(err)
	}
	locations, err := registry.Resolve(ctx, "tenant", "user", "one")
	if err != nil || len(locations) != 1 || locations[0].Generation != replacement.Generation {
		t.Fatalf("old disconnect removed new registration: %v %v", locations, err)
	}
	if err := registry.Renew(ctx, []routing.Location{first}, time.Second); !errors.Is(err, routing.ErrStale) {
		t.Fatalf("old renewal: %v", err)
	}
	res, err = router.SendToSession(ctx, "tenant", "user", "one", nil, "reconnect")
	if err != nil || res.Retryable() {
		t.Fatalf("reconnect: %+v %v", res, err)
	}
	res, err = router.SendToUser(ctx, "tenant", "offline", nil, "offline")
	if err != nil || !res.Offline {
		t.Fatalf("offline: %+v %v", res, err)
	}
	// Registry only advertises a ghost node: publish is not an enqueue ACK.
	_, err = registry.Register(ctx, routing.Location{TenantID: "tenant", UserID: "ghost", SessionID: "ghost", NodeID: "absent", Generation: "generation"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	res, err = router.SendToUser(ctx, "tenant", "ghost", nil, "lost-result")
	if !errors.Is(err, context.DeadlineExceeded) || len(res.Outcomes) != 1 || res.Outcomes[0].Status != routing.Unknown {
		t.Fatalf("expected unknown on lost result: %+v %v", res, err)
	}
	// Relocate after resolution, before the old gateway receives the command.
	moving := register(a, "tenant", "moving", "device", "old-shard", 2)
	moved := false
	staleTransport := interceptTransport{DeliveryTransport: sender, deliver: func(ctx context.Context, node string, c routing.DeliveryCommand) (routing.DeliveryResult, error) {
		if !moved {
			moved = true
			_, err := b.Register(ctx, routing.Location{TenantID: "tenant", UserID: "moving", SessionID: "device", ShardID: "new-shard"}, func([]byte) error { counts[2].Add(1); return nil }, nil)
			if err != nil {
				return routing.DeliveryResult{}, err
			}
			if err := a.Remove(ctx, moving); err != nil {
				return routing.DeliveryResult{}, err
			}
		}
		return sender.Deliver(ctx, node, c)
	}}
	staleRouter, _ := routing.NewRouter(routing.RouterConfig{Registry: registry, Transport: staleTransport})
	res, err = staleRouter.SendToSession(ctx, "tenant", "moving", "device", nil, "moving-message")
	if err != nil || res.Retryable() || res.Count(routing.Enqueued) != 1 || staleRouter.Stats().Attempts != 2 {
		t.Fatalf("stale retry: %+v %v", res, err)
	}
	// Lose a result after the destination actually enqueued, then retry the ID.
	before := counts[1].Load()
	lost := interceptTransport{DeliveryTransport: sender, deliver: func(ctx context.Context, node string, c routing.DeliveryCommand) (routing.DeliveryResult, error) {
		_, err := sender.Deliver(ctx, node, c)
		if err != nil {
			return routing.DeliveryResult{}, err
		}
		return routing.DeliveryResult{}, context.DeadlineExceeded
	}}
	lostRouter, _ := routing.NewRouter(routing.RouterConfig{Registry: registry, Transport: lost})
	res, err = lostRouter.SendToSession(ctx, "tenant", "user", "two", nil, "lost-after-enqueue")
	if err != nil || res.Count(routing.Unknown) != 1 {
		t.Fatalf("lost result: %+v %v", res, err)
	}
	res, err = router.SendToSession(ctx, "tenant", "user", "two", nil, "lost-after-enqueue")
	if err != nil || res.Retryable() || counts[1].Load() != before+1 {
		t.Fatalf("unknown retry duplicated enqueue: %+v %v", res, err)
	}

}

type interceptTransport struct {
	routing.DeliveryTransport
	deliver func(context.Context, string, routing.DeliveryCommand) (routing.DeliveryResult, error)
}

func (t interceptTransport) Deliver(ctx context.Context, node string, c routing.DeliveryCommand) (routing.DeliveryResult, error) {
	return t.deliver(ctx, node, c)
}

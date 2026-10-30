package redis_test

import (
	"context"
	"errors"
	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/websocket/routing"
	"github.com/thanhbvha/go-common/websocket/routing/contracttest"
	driver "github.com/thanhbvha/go-common/websocket/routing/redis"
	"testing"
	"time"
)

func TestContract(t *testing.T) {
	contracttest.Run(t, func(t *testing.T) (routing.SessionRegistry, func() routing.DeliveryTransport) {
		server := miniredis.RunT(t)
		client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
		t.Cleanup(func() { client.Close() })
		return driver.NewRegistry(client, "test"), func() routing.DeliveryTransport { return driver.NewTransport(client, "test") }
	})
}
func TestLeaseExpiryAndRenewal(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	r := driver.NewRegistry(client, "lease")
	ctx := context.Background()
	l, err := r.Register(ctx, routing.Location{TenantID: "t", UserID: "u", SessionID: "s", NodeID: "n", Generation: "g"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.JoinRoom(ctx, l, "room", time.Second); err != nil {
		t.Fatal(err)
	}
	server.FastForward(2 * time.Second)
	ls, err := r.Resolve(ctx, "t", "u", "")
	if err != nil || len(ls) != 0 {
		t.Fatalf("expired: %v %v", ls, err)
	}
	nodes, err := r.ResolveRoom(ctx, "t", "room")
	if err != nil || len(nodes) != 0 {
		t.Fatalf("room expired: %v %v", nodes, err)
	}
	if err = r.Renew(ctx, []routing.Location{l}, time.Second); err != nil {
		t.Fatal(err)
	}
	ls, err = r.Resolve(ctx, "t", "u", "")
	if err != nil || len(ls) != 1 {
		t.Fatalf("reconcile: %v %v", ls, err)
	}
}

func TestUserCapacityAndMixedLeaseTTLs(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	r := driver.NewRegistry(client, "capacity", driver.WithMaxSessionsPerUser(2))
	ctx := context.Background()
	location := routing.Location{TenantID: "t", UserID: "u", SessionID: "long", NodeID: "n", Generation: "g"}
	l, err := r.Register(ctx, location, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.JoinRoom(ctx, l, "room", time.Hour); err != nil {
		t.Fatal(err)
	}
	location.SessionID = "short"
	short, err := r.Register(ctx, location, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.JoinRoom(ctx, short, "room", time.Second); err != nil {
		t.Fatal(err)
	}
	location.SessionID = "third"
	if _, err = r.Register(ctx, location, time.Second); !errors.Is(err, routing.ErrCapacity) {
		t.Fatalf("capacity: %v", err)
	}
	server.FastForward(2 * time.Second)
	ls, err := r.Resolve(ctx, "t", "u", "long")
	if err != nil || len(ls) != 1 {
		t.Fatalf("short lease expired longer session: %v %v", ls, err)
	}
	nodes, err := r.ResolveRoom(ctx, "t", "room")
	if err != nil || len(nodes) != 1 {
		t.Fatalf("short room lease expired longer membership: %v %v", nodes, err)
	}
}

func TestRenewCannotBypassUserCapacity(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	r := driver.NewRegistry(client, "renew-cap", driver.WithMaxSessionsPerUser(1))
	ctx := context.Background()
	l := routing.Location{TenantID: "t", UserID: "u", SessionID: "first", NodeID: "n", Generation: "g"}
	if _, err := r.Register(ctx, l, time.Second); err != nil {
		t.Fatal(err)
	}
	l.SessionID = "second"
	if err := r.Renew(ctx, []routing.Location{l}, time.Second); !errors.Is(err, routing.ErrCapacity) {
		t.Fatalf("renew bypassed capacity: %v", err)
	}
}

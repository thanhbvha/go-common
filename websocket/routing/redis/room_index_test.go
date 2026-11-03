package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
)

func indexedRegistry(t *testing.T) (*Registry, *goredis.Client) {
	t.Helper()
	r, c := fastpathRegistry(t, 1000)
	return NewRegistry(c, r.prefix, WithRoomNodeIndex()), c
}
func joinIndexedSession(t *testing.T, r *Registry, id, node string, ttl time.Duration) routing.Location {
	t.Helper()
	l, err := r.Register(context.Background(), routing.Location{TenantID: "t", UserID: "u", SessionID: id, NodeID: node, Generation: "generation-" + id}, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.JoinRoom(context.Background(), l, "room", ttl); err != nil {
		t.Fatal(err)
	}
	return l
}

func TestRoomNodeIndexPreservesOtherMembersAndFencedReplacement(t *testing.T) {
	r, _ := indexedRegistry(t)
	ctx := context.Background()
	one := joinIndexedSession(t, r, "one", "node", time.Hour)
	two := joinIndexedSession(t, r, "two", "node", time.Minute)
	if err := r.LeaveRoom(ctx, one, "room"); err != nil {
		t.Fatal(err)
	}
	nodes, err := r.ResolveRoom(ctx, "t", "room")
	if err != nil || len(nodes) != 1 || nodes[0] != "node" {
		t.Fatalf("removed live node: %v %v", nodes, err)
	}
	fresh := two
	fresh.Generation = "replacement"
	if _, err = r.Register(ctx, fresh, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err = r.JoinRoom(ctx, fresh, "room", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err = r.RemoveSession(ctx, two, []string{"room"}); err != nil {
		t.Fatal(err)
	}
	nodes, err = r.ResolveRoom(ctx, "t", "room")
	if err != nil || len(nodes) != 1 {
		t.Fatalf("old generation removed replacement: %v %v", nodes, err)
	}
	if err = r.RemoveSession(ctx, fresh, []string{"room"}); err != nil {
		t.Fatal(err)
	}
	nodes, err = r.ResolveRoom(ctx, "t", "room")
	if err != nil || len(nodes) != 0 {
		t.Fatalf("last member leaked: %v %v", nodes, err)
	}
}

func TestRoomNodeIndexUsesMaximumLeaseAndPrunesExpiredMembers(t *testing.T) {
	r, c := indexedRegistry(t)
	ctx := context.Background()
	long := joinIndexedSession(t, r, "long", "node", time.Hour)
	short := joinIndexedSession(t, r, "short", "node", time.Minute)
	keys := r.roomKeys(long, "room")
	score, err := c.ZScore(ctx, keys[1], "node").Result()
	if err != nil || score < float64(time.Now().Add(59*time.Minute).UnixMilli()) {
		t.Fatal("short lease shortened node lease", err)
	}
	// Expire only the short member while the long member keeps the keys alive.
	if err = c.ZAdd(ctx, keys[2], goredis.Z{Score: float64(time.Now().Add(-time.Second).UnixMilli()), Member: member(short)}).Err(); err != nil {
		t.Fatal(err)
	}
	if err = r.LeaveRoom(ctx, long, "room"); err != nil {
		t.Fatal(err)
	}
	nodes, err := r.ResolveRoom(ctx, "t", "room")
	if err != nil || len(nodes) != 0 {
		t.Fatalf("expired member kept node alive: %v %v", nodes, err)
	}
}

func TestRoomNodeIndexResolveDoesNotInspectLegacyMemberSet(t *testing.T) {
	r, c := indexedRegistry(t)
	l := joinIndexedSession(t, r, "one", "node", time.Hour)
	ctx := context.Background()
	// A node-index lookup must not fetch/decode legacy recipient records.
	if err := c.ZAdd(ctx, r.roomKeys(l, "room")[0], goredis.Z{Score: float64(time.Now().Add(time.Hour).UnixMilli()), Member: "invalid legacy JSON"}).Err(); err != nil {
		t.Fatal(err)
	}
	nodes, err := r.ResolveRoom(ctx, "t", "room")
	if err != nil || len(nodes) != 1 || nodes[0] != "node" {
		t.Fatalf("indexed lookup read legacy recipients: %v %v", nodes, err)
	}
	if nodes, err = r.ResolveRoom(ctx, "other-tenant", "room"); err != nil || len(nodes) != 0 {
		t.Fatal("room index crossed tenants")
	}
}

func TestLegacyRoomResolutionAggregatesOnlyNodes(t *testing.T) {
	r, c := fastpathRegistry(t, 1000)
	ctx := context.Background()
	var members []goredis.Z
	for i := range 1000 {
		l := routing.Location{TenantID: "t", UserID: "u", SessionID: fmt.Sprint(i), NodeID: fmt.Sprint(i % 2), Generation: "old"}
		members = append(members, goredis.Z{Score: float64(time.Now().Add(time.Hour).UnixMilli()), Member: member(l)})
	}
	if err := c.ZAdd(ctx, r.key("t", "room", "room"), members...).Err(); err != nil {
		t.Fatal(err)
	}
	nodes, err := r.ResolveRoom(ctx, "t", "room")
	if err != nil || len(nodes) != 2 {
		t.Fatalf("legacy room node aggregation: %v %v", nodes, err)
	}
}

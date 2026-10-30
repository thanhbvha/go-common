package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/websocket/routing"
)

func fastpathRegistry(t *testing.T, capacity int) (*Registry, *goredis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRegistry(client, "fastpath", WithMaxSessionsPerUser(capacity)), client
}

func fastpathLocation(id string, expired bool) routing.Location {
	l := routing.Location{TenantID: "t", UserID: "u", SessionID: id, NodeID: "node", Generation: "generation", ExpiresAt: time.Now().Add(time.Hour)}
	if expired {
		l.ExpiresAt = time.Now().Add(-time.Hour)
	}
	return l
}

// Seed the existing on-Redis hash format without invoking any new writer code.
func seedLegacySessions(t *testing.T, r *Registry, c *goredis.Client, ls ...routing.Location) string {
	t.Helper()
	key := r.key("t", "user", "u")
	fields := make(map[string]interface{}, len(ls))
	for _, l := range ls {
		fields[l.SessionID] = encode(l)
	}
	if err := c.HSet(context.Background(), key, fields).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Expire(context.Background(), key, time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestRegisterAtCapacityPrunesLegacyExpiredSessions(t *testing.T) {
	r, client := fastpathRegistry(t, 2)
	ctx := context.Background()
	old := fastpathLocation("live", false)
	key := seedLegacySessions(t, r, client, old, fastpathLocation("expired", true))
	if _, err := r.Register(ctx, fastpathLocation("new", false), time.Minute); err != nil {
		t.Fatal(err)
	}
	if client.HLen(ctx, key).Val() != 2 || client.HExists(ctx, key, "expired").Val() {
		t.Fatal("expired record was counted against admission or leaked past capacity pruning")
	}
	if _, err := r.Register(ctx, fastpathLocation("overflow", false), time.Minute); !errors.Is(err, routing.ErrCapacity) {
		t.Fatalf("capacity bypass: %v", err)
	}
	fresh := old
	fresh.Generation = "replacement"
	if _, err := r.Register(ctx, fresh, time.Minute); err != nil {
		t.Fatal("live replacement consumed another slot", err)
	}
	if err := r.Renew(ctx, []routing.Location{old}, time.Minute); !errors.Is(err, routing.ErrStale) {
		t.Fatalf("old renewal was not fenced: %v", err)
	}
	if err := r.Remove(ctx, old); err != nil {
		t.Fatal(err)
	}
	locations, err := r.Resolve(ctx, "t", "u", "live")
	if err != nil || len(locations) != 1 || locations[0].Generation != fresh.Generation {
		t.Fatal("delayed cleanup removed the replacement", err)
	}
}

func TestRegisterExpiredSameIDRespectsCapacity(t *testing.T) {
	for _, capacity := range []int{1, 2} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			r, client := fastpathRegistry(t, capacity)
			ctx := context.Background()
			expired := fastpathLocation("expired", true)
			seedLegacySessions(t, r, client, expired, fastpathLocation("live", false))
			_, err := r.Register(ctx, expired, time.Minute)
			if capacity == 1 {
				if !errors.Is(err, routing.ErrCapacity) {
					t.Fatal("expired slot bypassed a lower capacity", err)
				}
				// Lowering a configured limit still permits replacing an existing
				// live session, as in the original registration contract.
				if _, err = r.Register(ctx, fastpathLocation("live", false), time.Minute); err != nil {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal("expired same-ID registration failed with room available", err)
			}
		})
	}
}

func TestResolveSinglePreservesLiveLeasesAndPrunesOnlyTarget(t *testing.T) {
	r, client := fastpathRegistry(t, 3)
	ctx := context.Background()
	key := seedLegacySessions(t, r, client, fastpathLocation("live", false), fastpathLocation("expired", true), fastpathLocation("other-expired", true))
	for _, id := range []string{"missing", "expired"} {
		ls, err := r.Resolve(ctx, "t", "u", id)
		if err != nil || len(ls) != 0 {
			t.Fatalf("invalid single-session resolution: %v %v", ls, err)
		}
	}
	if client.HExists(ctx, key, "expired").Val() || !client.HExists(ctx, key, "other-expired").Val() {
		t.Fatal("single-session cleanup touched unrelated fields")
	}
	ls, err := r.Resolve(ctx, "t", "u", "live")
	if err != nil || len(ls) != 1 || ls[0].SessionID != "live" {
		t.Fatal("live session lost", err)
	}
	// Another tenant with the same logical identifiers remains isolated.
	if ls, err = r.Resolve(ctx, "other-tenant", "u", "live"); err != nil || len(ls) != 0 {
		t.Fatal("tenant isolation lost", err)
	}
	if ls, err = r.Resolve(ctx, "t", "u", ""); err != nil || len(ls) != 1 || client.HLen(ctx, key).Val() != 1 {
		t.Fatal("user-wide resolution did not prune expired records", err)
	}
}

func TestConcurrentAdmissionCannotExceedCapacityAfterPruning(t *testing.T) {
	const capacity = 4
	r, client := fastpathRegistry(t, capacity)
	var expired []routing.Location
	for i := 0; i < capacity; i++ {
		expired = append(expired, fastpathLocation(fmt.Sprintf("expired-%d", i), true))
	}
	seedLegacySessions(t, r, client, expired...)
	var accepted atomic.Int64
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Go(func() {
			_, err := r.Register(context.Background(), fastpathLocation(fmt.Sprintf("new-%d", i), false), time.Minute)
			if err == nil {
				accepted.Add(1)
			} else if !errors.Is(err, routing.ErrCapacity) {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	ls, err := r.Resolve(context.Background(), "t", "u", "")
	if err != nil || accepted.Load() != capacity || len(ls) != capacity {
		t.Fatalf("admission race: accepted=%d sessions=%d err=%v", accepted.Load(), len(ls), err)
	}
}

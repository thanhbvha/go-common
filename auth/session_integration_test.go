//go:build integration

package auth

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisSessionIntegration(t *testing.T) {
	for _, mode := range []string{"standalone", "cluster"} {
		t.Run(mode, func(t *testing.T) {
			var client redis.UniversalClient
			address := os.Getenv("EVENTBUS_REDIS_ADDR")
			if mode == "cluster" {
				address = os.Getenv("EVENTBUS_REDIS_CLUSTER_ADDRS")
			}
			if address == "" {
				if os.Getenv("EVENTBUS_INTEGRATION_REQUIRED") == "1" {
					t.Fatalf("%s Redis fixture address is required", mode)
				}
				t.Skip("set EVENTBUS_REDIS_ADDR / EVENTBUS_REDIS_CLUSTER_ADDRS")
			}
			if mode == "cluster" {
				client = redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(address, ",")})
			} else {
				client = redis.NewClient(&redis.Options{Addr: address})
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := client.Ping(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			namespace, err := newTokenID()
			if err != nil {
				t.Fatal(err)
			}
			store, err := NewRedisSessionStore(SessionStoreOptions{Redis: client, Namespace: "auth-integration-" + namespace})
			if err != nil {
				t.Fatal(err)
			}
			session, err := store.Create(ctx, "user", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			key, _ := store.key(session.ID)
			defer client.Del(context.Background(), key)
			values, err := client.HGetAll(ctx, key).Result()
			if err != nil {
				t.Fatal(err)
			}
			for _, value := range values {
				if value == session.RefreshToken {
					t.Fatal("refresh token stored in plaintext")
				}
			}
			rotated, err := store.Rotate(ctx, session.ID, session.RefreshToken)
			if err != nil {
				t.Fatal(err)
			}
			if !rotated.ExpiresAt.Equal(session.ExpiresAt) {
				t.Fatal("rotation changed absolute expiry")
			}
			var wg sync.WaitGroup
			results := make(chan error, 2)
			for range 2 {
				wg.Add(1)
				go func() { defer wg.Done(); _, err := store.Rotate(ctx, rotated.ID, rotated.RefreshToken); results <- err }()
			}
			wg.Wait()
			close(results)
			successes, replays := 0, 0
			for err := range results {
				if err == nil {
					successes++
				} else if errors.Is(err, ErrRefreshReplay) {
					replays++
				} else {
					t.Fatal(err)
				}
			}
			if successes != 1 || replays != 1 {
				t.Fatalf("rotation successes=%d replays=%d", successes, replays)
			}
			revoked, err := store.IsRevoked(ctx, session.ID)
			if err != nil || !revoked {
				t.Fatalf("replayed session revoked=%v err=%v", revoked, err)
			}
			expiring, err := store.Create(ctx, "user", 100*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			expiryKey, _ := store.key(expiring.ID)
			defer client.Del(context.Background(), expiryKey)
			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for {
				revoked, err = store.IsRevoked(ctx, expiring.ID)
				if err != nil {
					t.Fatal(err)
				}
				if revoked {
					break
				}
				select {
				case <-tick.C:
				case <-deadline.C:
					t.Fatal("session did not expire")
				}
			}
		})
	}
}

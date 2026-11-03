package auth

import (
	"context"
	"errors"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"strings"
	"sync"
	"testing"
	"time"
)

func sessionFixture(t *testing.T) (*RedisSessionStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	store, err := NewRedisSessionStore(SessionStoreOptions{Redis: client, Namespace: "test", MaxRotations: 3})
	if err != nil {
		t.Fatal(err)
	}
	return store, mr
}
func TestSessionRotationHashesAndRejectsReplay(t *testing.T) {
	store, mr := sessionFixture(t)
	ctx := context.Background()
	first, err := store.Create(ctx, "user", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range mr.Keys() {
		values := mr.HGet(key, "current")
		if len(values) != 64 || strings.Contains(values, first.RefreshToken) {
			t.Fatalf("plaintext token stored: %q", values)
		}
	}
	next, err := store.Rotate(ctx, first.ID, first.RefreshToken)
	if err != nil || next.Subject != "user" || next.RefreshToken == first.RefreshToken {
		t.Fatalf("rotation=%+v %v", next, err)
	}
	if _, err = store.Rotate(ctx, first.ID, strings.Repeat("0", 64)); !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatal(err)
	}
	if revoked, err := store.IsRevoked(ctx, first.ID); err != nil || revoked {
		t.Fatalf("random input revoked session: %v %v", revoked, err)
	}
	if _, err = store.Rotate(ctx, first.ID, first.RefreshToken); !errors.Is(err, ErrRefreshReplay) {
		t.Fatalf("replay=%v", err)
	}
	if _, err = store.Rotate(ctx, first.ID, next.RefreshToken); !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatalf("replayed session still active: %v", err)
	}
}
func TestConcurrentSessionRotationAndExpiry(t *testing.T) {
	store, mr := sessionFixture(t)
	ctx := context.Background()
	first, err := store.Create(ctx, "u", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := store.Rotate(ctx, first.ID, first.RefreshToken); results <- err }()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successes=%d", successes)
	}
	revoked, err := store.IsRevoked(ctx, first.ID)
	if err != nil || !revoked {
		t.Fatalf("replayed concurrent session=%v %v", revoked, err)
	}
	second, err := store.Create(ctx, "u", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	mr.FastForward(time.Minute)
	if _, err = store.Rotate(ctx, second.ID, second.RefreshToken); !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatalf("expired=%v", err)
	}
}

func TestSessionsAreIsolatedAndBudgetIsBounded(t *testing.T) {
	store, _ := sessionFixture(t)
	ctx := context.Background()
	one, err := store.Create(ctx, "user", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	two, err := store.Create(ctx, "user", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		one, err = store.Rotate(ctx, one.ID, one.RefreshToken)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = store.Rotate(ctx, one.ID, one.RefreshToken); !errors.Is(err, ErrSessionExhausted) {
		t.Fatalf("budget error=%v", err)
	}
	revoked, err := store.IsRevoked(ctx, two.ID)
	if err != nil || revoked {
		t.Fatalf("second session affected: %v %v", revoked, err)
	}
	if err := store.Revoke(ctx, two.ID); err != nil {
		t.Fatal(err)
	}
	if revoked, err = store.IsRevoked(ctx, two.ID); err != nil || !revoked {
		t.Fatalf("logout=%v %v", revoked, err)
	}
	if _, err := store.Create(ctx, "", time.Hour); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, "user", 0); !errors.Is(err, ErrInvalidDuration) {
		t.Fatal(err)
	}
}

type delayedSessionReply struct{ after func() }

func (h delayedSessionReply) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h delayedSessionReply) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h delayedSessionReply) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() == "eval" {
			h.after()
		}
		return err
	}
}

func TestSessionExpiryDoesNotAddReplyLatency(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	now := time.Unix(1800000000, 0)
	mr.SetTime(now)
	client.AddHook(delayedSessionReply{after: func() { now = now.Add(time.Second); mr.SetTime(now); mr.FastForward(time.Second) }})
	store, err := NewRedisSessionStore(SessionStoreOptions{Redis: client, Namespace: "latency"})
	if err != nil {
		t.Fatal(err)
	}
	start := now
	session, err := store.Create(context.Background(), "user", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !session.ExpiresAt.Equal(start.Add(time.Hour)) {
		t.Fatalf("create added reply latency: %v", session.ExpiresAt)
	}
	rotated, err := store.Rotate(context.Background(), session.ID, session.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if !rotated.ExpiresAt.Equal(session.ExpiresAt) {
		t.Fatalf("rotation extended absolute lifetime: %v > %v", rotated.ExpiresAt, session.ExpiresAt)
	}
}

package cache

import (
	"context"
	"errors"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"sync"
	"testing"
	"time"
)

func TestNamespaceClearAndLeaseValidation(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer c.Close()
	ctx := context.Background()
	a, err := NewRedisCache(c, "a")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewRedisCache(c, "b")
	if err = a.Set(ctx, "key", "a", 0); err != nil {
		t.Fatal(err)
	}
	b.Set(ctx, "key", "b", 0)
	c.Set(ctx, "unrelated", "safe", 0)
	if err = a.Clear(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = a.Get(ctx, "key"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if value, _ := b.Get(ctx, "key"); value != "b" {
		t.Fatal("other cache deleted")
	}
	if value, _ := c.Get(ctx, "unrelated").Result(); value != "safe" {
		t.Fatal("unrelated data deleted")
	}
	lock, err := NewRedisLock(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, ttl := range []time.Duration{0, -1, time.Microsecond} {
		if _, err := lock.Acquire(ctx, "lease", ttl); err == nil {
			t.Fatal("invalid TTL accepted")
		}
	}
	if ok, err := lock.Acquire(ctx, "lease", time.Second); err != nil || !ok {
		t.Fatal(err)
	}
	if _, err := lock.Extend(ctx, "lease", time.Microsecond); err == nil {
		t.Fatal("invalid extension accepted")
	}
	mr.FastForward(time.Second)
	if _, err := lock.Acquire(ctx, "lease", time.Second); !errors.Is(err, ErrLockSessionUsed) {
		t.Fatal("ownership session reused", err)
	}
	next, _ := lock.NewSession()
	if ok, err := next.Acquire(ctx, "lease", time.Second); err != nil || !ok {
		t.Fatal(err)
	}
	if err := lock.Release(ctx, "lease"); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("stale session released", err)
	}
}

func TestMemoryLifecycleConcurrentClose(t *testing.T) {
	c, err := NewMemoryCache(1024, MemoryOptions{NumCounters: 100, Metrics: true})
	if err != nil {
		t.Fatal(err)
	}
	if c.Metrics() == nil {
		t.Fatal("metrics option ignored")
	}
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			for range 25 {
				_ = c.Set(context.Background(), "key", "value", time.Second)
				_, _ = c.Get(context.Background(), "key")
				_ = c.Delete(context.Background(), "key")
			}
		})
	}
	group.Go(func() { _ = c.Close() })
	group.Go(func() { _ = c.Close() })
	group.Wait()
	if _, err := c.Get(context.Background(), "key"); !errors.Is(err, ErrClosed) {
		t.Fatal("closed cache accepted read", err)
	}
}

func TestTypedNilRedisClientRejected(t *testing.T) {
	var client *redis.Client
	if _, err := NewRedisCache(client, "test"); err == nil {
		t.Fatal("typed nil cache client accepted")
	}
	if _, err := NewRedisLock(client); err == nil {
		t.Fatal("typed nil lock client accepted")
	}
}

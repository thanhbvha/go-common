package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisLockSessionOwnership(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), ContextTimeoutEnabled: true})
	defer client.Close()
	factory := NewRedisLock(client)
	first, err := factory.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	second, err := factory.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if ok, err := first.Acquire(ctx, "lease", time.Second); err != nil || !ok {
		t.Fatalf("first acquisition: %v", err)
	}
	mr.FastForward(time.Second)
	if ok, err := second.Acquire(ctx, "lease", time.Second); err != nil || !ok {
		t.Fatalf("second acquisition: %v", err)
	}
	if ok, err := first.Extend(ctx, "lease", time.Minute); err != nil || ok {
		t.Fatalf("stale session renewed new lease: %v", err)
	}
	if err := first.Release(ctx, "lease"); err == nil {
		t.Fatal("stale session released new lease")
	}
	if ok, err := second.Extend(ctx, "lease", time.Minute); err != nil || !ok {
		t.Fatalf("current owner could not renew: %v", err)
	}
	if err := second.Release(ctx, "lease"); err != nil {
		t.Fatal(err)
	}
}

func TestRedisLockSessionValidation(t *testing.T) {
	for _, lock := range []*RedisLock{nil, {}, NewRedisLock(nil)} {
		if _, err := lock.NewSession(); err == nil {
			t.Fatal("accepted nil lock/client")
		}
	}
}

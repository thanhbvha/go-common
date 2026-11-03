package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisLimiter_Allow(t *testing.T) {
	// Setup Miniredis
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	defer mr.Close()

	// Setup Redis Client
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer rdb.Close()

	// Setup Limiter
	limiter, err := NewRedisLimiter(rdb, "test_rate")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Limit:  3,
		Window: 1 * time.Second,
	}

	ctx := context.Background()
	key := "user_123"

	// 1st request
	res, err := limiter.Allow(ctx, key, cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !res.Allowed || res.Remaining != 2 {
		t.Errorf("Expected Allowed=true, Remaining=2, got %+v", res)
	}

	// 2nd request
	res, _ = limiter.Allow(ctx, key, cfg)
	if !res.Allowed || res.Remaining != 1 {
		t.Errorf("Expected Allowed=true, Remaining=1, got %+v", res)
	}

	// 3rd request
	res, _ = limiter.Allow(ctx, key, cfg)
	if !res.Allowed || res.Remaining != 0 {
		t.Errorf("Expected Allowed=true, Remaining=0, got %+v", res)
	}

	// 4th request (Should fail)
	res, _ = limiter.Allow(ctx, key, cfg)
	if res.Allowed || res.Remaining != 0 {
		t.Errorf("Expected Allowed=false, Remaining=0, got %+v", res)
	}

	// Fast forward time by 1 second using Miniredis fast forward
	mr.FastForward(1500 * time.Millisecond)

	// 5th request (Should succeed because window is reset)
	res, _ = limiter.Allow(ctx, key, cfg)
	if !res.Allowed || res.Remaining != 2 {
		t.Errorf("Expected Allowed=true after window reset, got %+v", res)
	}
}

func TestLimiterRejectsInvalidPolicyAndUsesMilliseconds(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	limiter, err := NewRedisLimiter(client, "ms")
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []Config{{Limit: 0, Window: time.Second}, {Limit: 1, Window: 0}, {Limit: -1, Window: time.Second}, {Limit: 1, Window: -time.Second}, {Limit: 1, Window: time.Microsecond}} {
		if _, err := limiter.Allow(context.Background(), "key", cfg); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
	result, err := limiter.Allow(context.Background(), "key", Config{Limit: 1, Window: 1500 * time.Millisecond})
	if err != nil || result.ResetAfter != 1500*time.Millisecond {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	mr.FastForward(1100 * time.Millisecond)
	result, err = limiter.Allow(context.Background(), "key", Config{Limit: 1, Window: 1500 * time.Millisecond})
	if err != nil || result.Allowed {
		t.Fatalf("window shortened: %+v %v", result, err)
	}
}

func TestLimiterReportsQuotaKeyWithoutExpiry(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	limiter, err := NewRedisLimiter(client, "bad-expiry")
	if err != nil {
		t.Fatal(err)
	}
	if err := mr.Set("bad-expiry:key", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := limiter.Allow(context.Background(), "key", Config{Limit: 1, Window: time.Second}); err == nil {
		t.Fatal("persistent quota key accepted as ordinary exhaustion")
	}
}

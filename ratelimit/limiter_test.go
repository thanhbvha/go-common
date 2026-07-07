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
	limiter := NewRedisLimiter(rdb, "test_rate")
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

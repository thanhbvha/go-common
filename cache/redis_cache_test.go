package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupRedisTestClient(t *testing.T) redis.UniversalClient {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestRedisCache_GetSetDelete(t *testing.T) {
	client := setupRedisTestClient(t)
	defer client.Close()

	ctx := context.Background()
	cache, err := NewRedisCache(client, "test")
	if err != nil {
		t.Fatal(err)
	}

	// Test Set
	err = cache.Set(ctx, "redis_test_key", "redis_value", 5*time.Second)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Test Get
	val, err := cache.Get(ctx, "redis_test_key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "redis_value" {
		t.Errorf("expected 'redis_value', got '%s'", val)
	}

	// Test Delete
	err = cache.Delete(ctx, "redis_test_key")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Test Get after Delete
	_, err = cache.Get(ctx, "redis_test_key")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

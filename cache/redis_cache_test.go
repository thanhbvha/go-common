package cache

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func setupRedisTestClient(t *testing.T) redis.UniversalClient {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping Redis test, could not connect to localhost:6379: %v", err)
	}
	
	return client
}

func TestRedisCache_GetSetDelete(t *testing.T) {
	client := setupRedisTestClient(t)
	defer client.Close()
	
	ctx := context.Background()
	cache := NewRedisCache(client)
	
	// Test Set
	err := cache.Set(ctx, "redis_test_key", "redis_value", 5*time.Second)
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

package cache

import (
	"context"
	"testing"
	"time"
)

func TestMemoryCache_GetSetDelete(t *testing.T) {
	ctx := context.Background()

	// 10MB cache
	cache, err := NewMemoryCache(10 * 1024 * 1024)
	if err != nil {
		t.Fatalf("failed to init cache: %v", err)
	}

	// Wait for cache to initialize internally (Ristretto initializes asynchronously)
	time.Sleep(10 * time.Millisecond)

	// Test Set
	err = cache.Set(ctx, "test_key", "test_value", 5*time.Second)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Ristretto sets are asynchronous, we need to wait briefly for it to be visible
	time.Sleep(50 * time.Millisecond)

	// Test Get
	val, err := cache.Get(ctx, "test_key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "test_value" {
		t.Errorf("expected 'test_value', got '%s'", val)
	}

	// Test Delete
	err = cache.Delete(ctx, "test_key")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	time.Sleep(10 * time.Millisecond) // async del

	// Test Get after Delete
	_, err = cache.Get(ctx, "test_key")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryCache_TTL(t *testing.T) {
	ctx := context.Background()
	cache, _ := NewMemoryCache(10 * 1024 * 1024)
	time.Sleep(10 * time.Millisecond)

	// Set with 100ms TTL
	cache.Set(ctx, "ttl_key", "val", 100*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	val, err := cache.Get(ctx, "ttl_key")
	if err != nil || val != "val" {
		t.Fatalf("expected key to exist, err: %v", err)
	}

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	_, err = cache.Get(ctx, "ttl_key")
	if err != ErrNotFound {
		t.Fatalf("expected key to be expired and return ErrNotFound, got %v", err)
	}
}

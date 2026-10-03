package cache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/cache"
)

type testUser struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestGetSetJSON(t *testing.T) {
	c, _ := cache.NewMemoryCache(10 * 1024 * 1024)
	defer c.Close()
	ctx := context.Background()

	in := testUser{ID: 1, Name: "Alice"}
	if err := cache.SetJSON(ctx, c, "user:1", in, time.Minute); err != nil {
		t.Fatalf("SetJSON: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	var out testUser
	if err := cache.GetJSON(ctx, c, "user:1", &out); err != nil {
		t.Fatalf("GetJSON: %v", err)
	}
	if out.ID != 1 || out.Name != "Alice" {
		t.Errorf("unexpected value: %+v", out)
	}
}

func TestGetJSONNotFound(t *testing.T) {
	c, _ := cache.NewMemoryCache(10 * 1024 * 1024)
	defer c.Close()

	var u testUser
	err := cache.GetJSON(context.Background(), c, "missing", &u)
	if !errors.Is(err, cache.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

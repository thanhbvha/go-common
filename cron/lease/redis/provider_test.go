package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func TestProviderSessionIsolation(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer client.Close()
	provider := New(client)
	first, err := provider.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	second, err := provider.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if ok, err := first.Acquire(ctx, "key", time.Second); err != nil || !ok {
		t.Fatal(err)
	}
	mr.FastForward(time.Second)
	if ok, err := second.Acquire(ctx, "key", time.Second); err != nil || !ok {
		t.Fatal(err)
	}
	if err := first.Release(ctx, "key"); err == nil {
		t.Fatal("stale owner released successor")
	}
	for _, p := range []*Provider{nil, {}, New(nil)} {
		if _, err := p.NewSession(); err == nil {
			t.Fatal("nil client accepted")
		}
	}
}

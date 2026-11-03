package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func TestLifecycleHealthAndTerminalClose(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := DefaultConfig()
	cfg.Host = mr.Host()
	cfg.Port, _ = strconv.Atoi(mr.Port())
	cfg.MaxConnRetries = 1
	c := New(cfg)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	native := c.Native()
	c.mu.Lock()
	c.connected = false
	c.mu.Unlock()
	if err := c.Connect(context.Background()); err != nil || c.Native() != native {
		t.Fatal("unhealthy probe replaced pool", err)
	}
	if err := c.Set(context.Background(), "key", "value", time.Second); err != nil {
		t.Fatal("health flag blocked command", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Connect(context.Background()); !errors.Is(err, goredis.ErrClosed) {
		t.Fatalf("Connect after Close: %v", err)
	}
}
func TestClientValidationSecurityAndRetryCancellation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Mode = 99
	if err := New(cfg).Connect(context.Background()); err == nil {
		t.Fatal("invalid topology accepted")
	}
	cfg = DefaultConfig()
	cfg.Username = "app"
	cfg.Password = "password"
	cfg.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	c := New(cfg)
	native, err := c.buildClient()
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	opts := native.(*goredis.Client).Options()
	if opts.Username != "app" || opts.TLSConfig == nil || !opts.ContextTimeoutEnabled {
		t.Fatal("security/context settings lost")
	}
	cfg = DefaultConfig()
	cfg.Port = 1
	cfg.MaxConnRetries = 10
	cfg.MaxRetries = -1
	cfg.DialTimeout = 20 * time.Millisecond
	c = New(cfg)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := c.Connect(ctx); err == nil {
		t.Fatal("unexpected connection")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("startup retry ignored cancellation")
	}
}

func TestCounterCreationTTLIsAtomicAndDoesNotRefresh(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := DefaultConfig()
	cfg.Host = mr.Host()
	cfg.Port, _ = strconv.Atoi(mr.Port())
	c := New(cfg)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	if n, err := c.IncrBy(ctx, "counter", 0, time.Second); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if ttl := mr.TTL("counter"); ttl != time.Second {
		t.Fatal("missing initial TTL", ttl)
	}
	mr.FastForward(500 * time.Millisecond)
	if _, err := c.Incr(ctx, "counter", time.Second); err != nil {
		t.Fatal(err)
	}
	if ttl := mr.TTL("counter"); ttl != 500*time.Millisecond {
		t.Fatal("existing TTL was refreshed", ttl)
	}
	if n, err := c.IncrByFloat(ctx, "float", 1.25, time.Second); err != nil || n != 1.25 {
		t.Fatal(n, err)
	}
}

func TestCounterLuaPreservesFullIntegerPrecision(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := DefaultConfig()
	cfg.Host = mr.Host()
	cfg.Port, _ = strconv.Atoi(mr.Port())
	client := New(cfg)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if value, err := client.IncrBy(context.Background(), "large", math.MaxInt64-1, time.Minute); err != nil || value != math.MaxInt64-1 {
		t.Fatal("counter precision lost", value, err)
	}
	if value, err := client.Incr(context.Background(), "large", time.Minute); err != nil || value != math.MaxInt64 {
		t.Fatal("counter increment precision lost", value, err)
	}
}

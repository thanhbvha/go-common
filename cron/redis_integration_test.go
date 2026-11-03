//go:build integration

package cron

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/v2/cache"
)

func realRedisFixture(t *testing.T) (*goredis.Client, func()) {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		if os.Getenv("REDIS_INTEGRATION_REQUIRED") == "1" {
			t.Fatal("redis-server required", err)
		}
		t.Skip("redis-server unavailable")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	dir := t.TempDir()
	log, err := os.Create(filepath.Join(dir, "redis.log"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "--bind", "127.0.0.1", "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no", "--dir", dir)
	command.Stdout = log
	command.Stderr = log
	if err = command.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { _ = command.Process.Kill(); _ = command.Wait(); _ = log.Close() }) }
	t.Cleanup(stop)
	client := goredis.NewClient(&goredis.Options{Addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), ContextTimeoutEnabled: true, MaxRetries: -1, DialTimeout: 100 * time.Millisecond, ReadTimeout: 100 * time.Millisecond, WriteTimeout: 100 * time.Millisecond})
	t.Cleanup(func() { client.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if client.Ping(context.Background()).Err() == nil {
			return client, stop
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("disposable Redis did not start")
	return nil, nil
}

func TestDistributedRealRedisRenewalAndProcessLossIntegration(t *testing.T) {
	client, stopRedis := realRedisFixture(t)
	lock, err := cache.NewRedisLock(client)
	if err != nil {
		t.Fatal(err)
	}
	provider := &testLeaseProvider{lock}
	var count atomic.Int32
	started := make(chan struct{})
	results := make(chan Result, 2)
	cfg := config(func(ctx context.Context) error { count.Add(1); close(started); <-ctx.Done(); return ctx.Err() })
	cfg.LockTTL = 600 * time.Millisecond
	cfg.LockTimeout = 80 * time.Millisecond
	a := NewScheduler(append(quietOptions(), WithObserver(func(r Result) { results <- r }))...)
	b := NewScheduler(quietOptions()...)
	register(t, a, cfg, provider)
	register(t, b, cfg, provider)
	at := time.Now()
	done := invocation(a, at)
	wait(t, started)
	// Stay active beyond several original lease lifetimes; renewals must retain
	// ownership and a later slot on another replica must skip overlap.
	until := time.Now().Add(1400 * time.Millisecond)
	for time.Now().Before(until) {
		ttl, err := client.PTTL(context.Background(), "cron:lock:report").Result()
		if err != nil || ttl <= 0 {
			t.Fatal("lease expired despite renewal", ttl, err)
		}
		time.Sleep(80 * time.Millisecond)
	}
	wait(t, invocation(b, at.Add(time.Second)))
	if count.Load() != 1 {
		t.Fatal("overlapping callback")
	}
	stopRedis()
	wait(t, done)
	result := <-results
	if result.Status != "lease_lost" || !errors.Is(result.Err, ErrLeaseLost) {
		t.Fatalf("Redis process failure did not cancel owner: %+v", result)
	}
}

func TestRedisLeaseCrashOwnerHelper(t *testing.T) {
	addr := os.Getenv("CRON_CRASH_OWNER_ADDR")
	if addr == "" {
		return
	}
	client := goredis.NewClient(&goredis.Options{Addr: addr, MaxRetries: -1})
	defer client.Close()
	lease, err := cache.NewRedisLock(client)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := lease.Acquire(context.Background(), "crashed-owner", 400*time.Millisecond); err != nil || !ok {
		t.Fatal("cannot acquire crash fixture lease", err)
	}
	// Exit without releasing: the Redis TTL, not process cleanup, must recover it.
	os.Exit(0)
}
func TestRedisLeaseOwnerCrashExpiresIntegration(t *testing.T) {
	client, _ := realRedisFixture(t)
	child := exec.Command(os.Args[0], "-test.run=^TestRedisLeaseCrashOwnerHelper$")
	child.Env = append(os.Environ(), "CRON_CRASH_OWNER_ADDR="+client.Options().Addr, "GORACE=atexit_sleep_ms=0")
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("owner helper: %s %v", output, err)
	}
	if ttl, err := client.PTTL(context.Background(), "crashed-owner").Result(); err != nil || ttl <= 0 {
		t.Fatal("helper did not leave a live lease", ttl, err)
	}
	deadline := time.Now().Add(3 * time.Second)
	lease, err := cache.NewRedisLock(client)
	if err != nil {
		t.Fatal(err)
	}
	for time.Now().Before(deadline) {
		ok, err := lease.Acquire(context.Background(), "crashed-owner", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("crashed owner lease never expired")
}

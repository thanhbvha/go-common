package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/websocket/routing"
)

// BenchmarkRegistryHash requires an explicitly selected disposable Redis. It
// never flushes a database: each case owns one UUID-namespaced hash with a TTL.
// WS_REGISTRY_BENCH_ADDR=127.0.0.1:6379 go test -run '^$'
// -bench '^BenchmarkRegistryHash$' -benchtime=1000x -count=3 -benchmem ./websocket/routing/redis
func BenchmarkRegistryHash(b *testing.B) {
	addr := os.Getenv("WS_REGISTRY_BENCH_ADDR")
	if addr == "" {
		b.Skip("set WS_REGISTRY_BENCH_ADDR to a disposable Redis instance")
	}
	client := goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv("WS_REGISTRY_BENCH_PASSWORD"), MaxRetries: -1, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, PoolSize: 4})
	b.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		b.Fatal("benchmark Redis is unavailable")
	}
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("sessions=%d", n), func(b *testing.B) {
			for _, expired := range []bool{false, true} {
				percent := 0
				if expired {
					percent = 50
				}
				b.Run(fmt.Sprintf("expired=%d", percent), func(b *testing.B) {
					for _, op := range []string{"ResolveHit", "ResolveMiss", "ResolveAll", "RegisterReplace", "RegisterNew", "RegisterCapacity", "HGETControl"} {
						if expired && (op == "RegisterCapacity" || op == "HGETControl") {
							continue
						}
						b.Run(op, func(b *testing.B) { benchmarkRegistryCase(b, client, n, expired, op, false) })
						if !expired && (strings.HasPrefix(op, "Resolve") || op == "RegisterReplace" || op == "HGETControl") {
							b.Run(op+"Parallel", func(b *testing.B) { benchmarkRegistryCase(b, client, n, false, op, true) })
						}
					}
				})
			}
		})
	}
}

func benchmarkRegistryCase(b *testing.B, client *goredis.Client, n int, expired bool, op string, parallel bool) {
	ctx := context.Background()
	r := NewRegistry(client, "ws-registry-bench:"+routing.NewID())
	if op == "RegisterNew" {
		r.maxSessions = n + 1 // Exactly one slot of admission headroom, including N=1000.
	} else if op == "RegisterCapacity" {
		r.maxSessions = n // Exercise rejection at every cardinality.
	}
	key := r.key("bench-tenant", "user", "bench-user")
	b.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Del(cleanupCtx, key).Err(); err != nil {
			b.Error("could not clean benchmark-owned hash; TTL remains")
		}
	})
	fields := make(map[string]interface{}, n)
	var target routing.Location
	dead := 0
	if expired {
		dead = n / 2 // N=1 has zero expired entries.
	}
	for i := 0; i < n; i++ {
		l := routing.Location{TenantID: "bench-tenant", UserID: "bench-user", SessionID: fmt.Sprintf("session-%04d", i), NodeID: "bench-node", ShardID: "bench-shard", Generation: "bench-generation", ExpiresAt: time.Now().Add(time.Hour)}
		if i < dead {
			l.ExpiresAt = time.Now().Add(-time.Hour)
		}
		fields[l.SessionID] = encode(l)
		target = l // Last session is always live.
	}
	seed := func() {
		pipe := client.Pipeline()
		pipe.Del(ctx, key)
		pipe.HSet(ctx, key, fields)
		pipe.Expire(ctx, key, time.Hour)
		if _, err := pipe.Exec(ctx); err != nil {
			b.Fatal("cannot seed benchmark hash")
		}
	}
	seed()
	operation := func() error {
		switch op {
		case "ResolveHit", "ResolveMiss", "ResolveAll":
			session, want := target.SessionID, 1
			if op == "ResolveMiss" {
				session, want = "missing-session", 0
			} else if op == "ResolveAll" {
				session, want = "", n-dead
			}
			locations, err := r.Resolve(ctx, target.TenantID, target.UserID, session)
			if err != nil {
				return err
			}
			if len(locations) != want {
				return fmt.Errorf("unexpected result count: got %d want %d", len(locations), want)
			}
			return nil
		case "RegisterReplace", "RegisterNew", "RegisterCapacity":
			l := target
			if op != "RegisterReplace" {
				l.SessionID = "new-session"
			}
			_, err := r.Register(ctx, l, time.Hour)
			if op == "RegisterCapacity" {
				if errors.Is(err, routing.ErrCapacity) {
					return nil
				}
				return errors.New("expected capacity rejection")
			}
			return err
		case "HGETControl":
			// Raw lookup only: not equivalent to Resolve's expiry contract.
			return client.HGet(ctx, key, target.SessionID).Err()
		}
		return errors.New("unknown benchmark operation")
	}
	// Warm connections, script and Go paths; restore the fixture afterward.
	if err := operation(); err != nil {
		b.Fatal("benchmark warmup failed")
	}
	seed()
	latencies := make([]int64, b.N)
	beforeCalls, beforeMicros := benchmarkEvalStats(b, client)
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		var next atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i := int(next.Add(1) - 1)
				start := time.Now()
				err := operation()
				latencies[i] = time.Since(start).Nanoseconds()
				if err != nil {
					b.Error("benchmark operation failed")
				}
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			if i > 0 && (expired || op == "RegisterNew") {
				b.StopTimer()
				seed() // Restore expired records/admission headroom for EVERY sample.
				b.StartTimer()
			}
			start := time.Now()
			err := operation()
			latencies[i] = time.Since(start).Nanoseconds()
			if err != nil {
				b.Fatal("benchmark operation failed")
			}
		}
	}
	b.StopTimer()
	afterCalls, afterMicros := benchmarkEvalStats(b, client)
	if op != "HGETControl" {
		if afterCalls-beforeCalls != int64(b.N) {
			b.Fatal("Redis EVAL stats contaminated by another workload")
		}
		b.ReportMetric(float64(afterMicros-beforeMicros)/float64(b.N), "redis-eval-us/op")
	}
	slices.Sort(latencies)
	// Go's testing package uses QPC on Windows, whereas time.Now has coarser
	// granularity there. Keep ns/op valid but only publish precise percentiles
	// on other systems; use the Linux binary for the complete measurement set.
	if runtime.GOOS != "windows" {
		for _, p := range []int{50, 95, 99} {
			index := max(0, (b.N*p+99)/100-1)
			b.ReportMetric(float64(latencies[index])/1000, fmt.Sprintf("p%d-us", p))
		}
	}
	b.ReportMetric(1e9/float64(b.Elapsed().Nanoseconds())*float64(b.N), "ops/s")
}

func benchmarkEvalStats(b *testing.B, client *goredis.Client) (calls, micros int64) {
	b.Helper()
	info, err := client.Info(context.Background(), "commandstats").Result()
	if err != nil {
		b.Fatal("cannot read benchmark Redis commandstats")
	}
	for _, line := range strings.Split(info, "\n") {
		if !strings.HasPrefix(line, "cmdstat_eval:") {
			continue
		}
		for _, field := range strings.Split(strings.TrimSpace(strings.TrimPrefix(line, "cmdstat_eval:")), ",") {
			parts := strings.SplitN(field, "=", 2)
			if len(parts) != 2 {
				continue
			}
			value, _ := strconv.ParseInt(parts[1], 10, 64)
			switch parts[0] {
			case "calls":
				calls = value
			case "usec":
				micros = value
			}
		}
	}
	return
}

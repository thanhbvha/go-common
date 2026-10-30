package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/websocket/routing"
)

// BenchmarkRegistryRegisterRenew uses only an explicitly selected disposable Redis.
// A batch operation renews all N sessions, in the implementation's 256-command chunks.
func BenchmarkRegistryRegisterRenew(b *testing.B) {
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
			for _, profile := range []string{"RenewOne", "RenewOneParallel", "RenewAllBatch", "RenewAllBatchParallel", "RenewAllBatchStale10", "RegisterChurn", "RegisterChurnParallel", "MixedRegister1Percent", "MixedRegister10Percent", "MixedRegister50Percent", "RegisterWithBatchRenew"} {
				b.Run(profile, func(b *testing.B) { benchmarkRegisterRenewCase(b, client, n, profile) })
			}
		})
	}
}

type registerRenewFixture struct {
	r        *Registry
	mu       sync.RWMutex
	ls       []routing.Location
	register []sync.Mutex
}

func (f *registerRenewFixture) snapshot(index int, all bool) []routing.Location {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if all {
		return slices.Clone(f.ls)
	}
	return []routing.Location{f.ls[index]}
}

func (f *registerRenewFixture) replace(index int) error {
	// Order local generation publication for concurrent replacements of the same
	// session. Different sessions can register concurrently. No registry I/O holds mu.
	f.register[index].Lock()
	defer f.register[index].Unlock()
	l := f.snapshot(index, false)[0]
	l.Generation = routing.NewID()
	registered, err := f.r.Register(context.Background(), l, time.Hour)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.ls[index] = registered
	f.mu.Unlock()
	return nil
}

func (f *registerRenewFixture) renew(index int, all, allowStale bool, wantStale int) (int, int, error) {
	ls := f.snapshot(index, all)
	results := f.r.RenewBatch(context.Background(), ls, time.Hour)
	if len(results) != len(ls) {
		return 0, 0, errors.New("incomplete renewal results")
	}
	stale := 0
	for _, result := range results {
		if errors.Is(result.Err, routing.ErrStale) {
			stale++
		} else if result.Err != nil {
			return 0, 0, result.Err
		}
	}
	if !allowStale && stale != wantStale {
		return 0, 0, errors.New("unexpected stale generation count")
	}
	return len(ls), stale, nil
}

type registerRenewSample struct {
	kind byte // 'r' register or 'n' renewal API call
	ns   int64
}

func benchmarkRegisterRenewCase(b *testing.B, client *goredis.Client, n int, profile string) {
	ctx := context.Background()
	f := &registerRenewFixture{r: NewRegistry(client, "ws-register-renew-bench:"+routing.NewID()), register: make([]sync.Mutex, n)}
	key := f.r.key("bench-tenant", "user", "bench-user")
	b.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if client.Del(cleanupCtx, key).Err() != nil {
			b.Error("cannot clean benchmark-owned hash; TTL remains")
		}
	})
	staleFixture := 0
	if profile == "RenewAllBatchStale10" {
		staleFixture = n / 10
	}
	fields := make(map[string]interface{}, n)
	for i := 0; i < n; i++ {
		l := routing.Location{TenantID: "bench-tenant", UserID: "bench-user", SessionID: fmt.Sprintf("session-%04d", i), NodeID: "bench-node", ShardID: "bench-shard", Generation: routing.NewID(), ExpiresAt: time.Now().Add(time.Hour)}
		f.ls = append(f.ls, l)
		if i < staleFixture {
			l.Generation = routing.NewID()
		}
		fields[l.SessionID] = encode(l)
	}
	pipe := client.Pipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, time.Hour)
	if _, err := pipe.Exec(ctx); err != nil {
		b.Fatal("cannot seed benchmark hash")
	}
	registerPercent := 0
	switch profile {
	case "MixedRegister1Percent":
		registerPercent = 1
	case "MixedRegister10Percent":
		registerPercent = 10
	case "MixedRegister50Percent":
		registerPercent = 50
	}
	background := profile == "RegisterWithBatchRenew"
	parallel := strings.HasSuffix(profile, "Parallel") || registerPercent > 0
	all := strings.HasPrefix(profile, "RenewAllBatch")
	mixed := registerPercent > 0 || background
	var registerCalls, renewCalls, renewSessions, staleSessions atomic.Int64
	samples := make([]registerRenewSample, b.N)
	operation := func(i int) {
		start := time.Now()
		// 37 is coprime to 100: spread a deterministic exact percentage over
		// each block instead of putting every registration in one long burst.
		isRegister := strings.HasPrefix(profile, "Register") || (registerPercent > 0 && (i*37)%100 < registerPercent)
		if isRegister {
			err := f.replace(i % n)
			samples[i] = registerRenewSample{'r', time.Since(start).Nanoseconds()}
			if err != nil {
				b.Error("registration operation failed")
			}
			registerCalls.Add(1)
		} else {
			count, stale, err := f.renew(i%n, all, mixed, staleFixture)
			samples[i] = registerRenewSample{'n', time.Since(start).Nanoseconds()}
			if err != nil {
				b.Error("renewal operation failed")
			}
			renewCalls.Add(1)
			renewSessions.Add(int64(count))
			staleSessions.Add(int64(stale))
		}
	}
	// Warm both script paths and the connection before commandstats/timing.
	if _, _, err := f.renew(0, true, false, staleFixture); err != nil {
		b.Fatal("renewal warmup failed")
	}
	if staleFixture == 0 {
		if err := f.replace(0); err != nil {
			b.Fatal("registration warmup failed")
		}
	}
	beforeCalls, beforeMicros := benchmarkEvalStats(b, client)
	var backgroundTimes []int64 // Written only by the background worker.
	b.ReportAllocs()
	b.ResetTimer()
	if background {
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				start := time.Now()
				count, stale, err := f.renew(0, true, true, 0)
				backgroundTimes = append(backgroundTimes, time.Since(start).Nanoseconds())
				if err != nil {
					b.Error("background renewal failed")
				}
				renewCalls.Add(1)
				renewSessions.Add(int64(count))
				staleSessions.Add(int64(stale))
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
		var next atomic.Int64
		var workers sync.WaitGroup
		for range 3 {
			workers.Go(func() {
				for {
					i := int(next.Add(1) - 1)
					if i >= b.N {
						return
					}
					operation(i)
				}
			})
		}
		workers.Wait()
		close(stop)
		<-done
	} else if parallel {
		var next atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				operation(int(next.Add(1) - 1))
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			operation(i)
		}
	}
	b.StopTimer()
	afterCalls, afterMicros := benchmarkEvalStats(b, client)
	actions := registerCalls.Load() + renewSessions.Load()
	if afterCalls-beforeCalls != actions {
		b.Fatal("unexpected EVAL count or competing workload")
	}
	// Validate cardinality and current ownership outside the timed region.
	locations, err := f.r.Resolve(ctx, "bench-tenant", "bench-user", "")
	if err != nil || len(locations) != n {
		b.Fatal("workload lost session records")
	}
	if staleFixture == 0 {
		owners := make(map[string]string, n)
		for _, l := range locations {
			owners[l.SessionID] = l.Generation
		}
		for _, l := range f.snapshot(0, true) {
			if owners[l.SessionID] != l.Generation {
				b.Fatal("renewal overwrote a replacement generation")
			}
		}
	}
	var registerTimes, renewTimes []int64
	var allTimes []int64
	for _, sample := range samples {
		allTimes = append(allTimes, sample.ns)
		if sample.kind == 'r' {
			registerTimes = append(registerTimes, sample.ns)
		} else {
			renewTimes = append(renewTimes, sample.ns)
		}
	}
	renewTimes = append(renewTimes, backgroundTimes...)
	benchmarkWorkloadPercentiles(b, "api", allTimes)
	benchmarkWorkloadPercentiles(b, "register", registerTimes)
	benchmarkWorkloadPercentiles(b, "renew", renewTimes)
	seconds := b.Elapsed().Seconds()
	b.ReportMetric(float64(registerCalls.Load())/seconds, "register/s")
	b.ReportMetric(float64(renewCalls.Load())/seconds, "renew-calls/s")
	b.ReportMetric(float64(renewSessions.Load())/seconds, "renew-sessions/s")
	b.ReportMetric(float64(actions)/seconds, "session-actions/s")
	b.ReportMetric(float64(afterMicros-beforeMicros)/float64(actions), "redis-eval-us/call")
	if renewSessions.Load() > 0 {
		b.ReportMetric(100*float64(staleSessions.Load())/float64(renewSessions.Load()), "stale-percent")
	}
	b.ReportMetric(float64(registerCalls.Load()), "register-calls")
	b.ReportMetric(float64(renewCalls.Load()), "renew-calls")
}

func benchmarkWorkloadPercentiles(b *testing.B, kind string, ns []int64) {
	if len(ns) == 0 || runtime.GOOS == "windows" {
		return
	}
	slices.Sort(ns)
	for _, p := range []int{50, 95, 99} {
		index := max(0, (len(ns)*p+99)/100-1)
		b.ReportMetric(float64(ns[index])/1000, fmt.Sprintf("%s-p%d-us", kind, p))
	}
}

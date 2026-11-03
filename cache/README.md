# cache

Operational maturity, defaults and adoption gates are tracked in the [production matrix](../docs/PRODUCTION.md).

Independent local Ristretto and remote Redis caches implement `Cache`. This
package does not coordinate L1/L2 invalidation or provide database consistency.

```go
local, err := cache.NewMemoryCache(16*1024*1024,
    cache.MemoryOptions{NumCounters: 100_000, BufferItems: 64, Metrics: true})
if err != nil { return err }
defer local.Close()

remote, err := cache.NewRedisCache(redisClient, "orders:production")
if err != nil { return err }
if err := remote.Set(ctx, "order:123", "cached-value", time.Minute); err != nil { return err }
value, err := remote.Get(ctx, "order:123")
```

Use `github.com/thanhbvha/go-common/v2/cache`. Each Redis cache requires a nonempty
namespace. Keys use an encoded namespace prefix; `Clear(ctx)` scans and deletes
that prefix on each Cluster master and never calls FLUSHDB. Concurrent writes can
survive a clear; this is not an atomic invalidation barrier. Redis clients are
borrowed: cache Close does not close the shared pool.

TTL zero means no expiry; negative TTL is rejected. Get returns `ErrNotFound` for
missing values. Local writes are admitted asynchronously and may be dropped by
Ristretto policy; a successful Set is not durable storage. `MaxCost` includes
value and entry costs, not total process RSS. Admission sketches and queues add
memory overhead. Counter defaults scale with the requested capacity and are
capped at one million; use MemoryOptions to tune known workloads and measure RSS.
Metrics() returns optional counters. Local Close is terminal and concurrent-safe;
operations afterward return ErrClosed. Clear is serialized against operations.

## Redis ownership sessions

```go
factory, err := cache.NewRedisLock(redisClient)
if err != nil { return err }
lease, err := factory.NewSession() // Create per execution.
if err != nil { return err }
acquired, err := lease.Acquire(ctx, "orders:lease:123", 30*time.Second)
if err != nil { return err }
if !acquired { return nil }
defer func() {
    cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()
    _ = lease.Release(cleanup, "orders:lease:123")
}()
```

NewRedisLock(client) returns (*RedisLock, error). Constructor/session creation
returns ErrEntropyUnavailable (wrapping the underlying cause) if ownership-token
entropy cannot be read; nil/typed-nil clients are rejected before entropy reads.
Acquire/Extend require TTL >=1ms. An ownership session may successfully acquire a
particular key only once; an ambiguous acquisition also consumes that session.
Create a fresh NewSession for retry/reacquisition after expiry or release.
`ErrLockSessionUsed` prevents stale work from reusing ownership of a later lease.
Release/Extend compare the random token atomically. Lost ownership returns
`ErrLeaseLost` from Release, or `(false, nil)` from Extend. Contention returns
`(false, nil)` from Acquire. Constructors borrow the Redis client.

A single Redis lease does not fence already-running work, guarantee exactly-once
business effects, or survive failover as consensus. Business mutations need
idempotency keys/unique constraints or fencing. Use the cron Redis adapter for
renewable scheduled work; it cancels callbacks when ownership becomes uncertain.

# redis

Operational maturity, defaults and adoption gates are tracked in the [production matrix](../docs/PRODUCTION.md).

```go
import "github.com/thanhbvha/go-common/v2/redis"

// Standalone
cfg := redis.DefaultConfig()
cfg.Host     = "redis.internal"
cfg.Password = os.Getenv("REDIS_PASSWORD")
cfg.Prefix   = "myapp:"
cfg.Logger   = log // any logger.Logger-compatible value

client := redis.New(cfg)
if err := client.Connect(ctx); err != nil {
    log.Fatal(err)
}
redis.SetDefault(client)
defer redis.Close()

// Cluster
cfg.Mode         = redis.ModeCluster
cfg.ClusterAddrs = []string{"node1:6379", "node2:6379", "node3:6379"}

// Sentinel
cfg.Mode           = redis.ModeSentinel
cfg.SentinelAddrs  = []string{"sentinel1:26379"}
cfg.MasterName     = "mymaster"

// Usage
client.Set(ctx, client.BuildKey("session:abc"), data, time.Hour)
val, _  := client.Get(ctx, client.BuildKey("session:abc"))
version, _ := client.ServerVersion(ctx) // "7.2.4"
```

### Key types

| Symbol | Description |
|---|---|
| `Config` | All connection, pool, timeout, retry options |
| `ConnectionMode` | `ModeSingle` \| `ModeCluster` \| `ModeSentinel` |
| `Client` | Wraps `redis.UniversalClient`, thread-safe |
| `New(cfg)` | Allocate (does not connect) |
| `MustConnect(ctx, cfg)` | Allocate + connect, panic on error |
| `SetDefault(c)` | Register process-wide default |
| `Default()` | Retrieve default (panics if unset) |


## Lifecycle and secure deployments

Start with DefaultConfig and override credentials and endpoints. Config.Validate
runs before dialing. Set Username and Password for ACL authentication; use a
verified TLSConfig (custom RootCAs when needed). SentinelUsername and
SentinelPassword authenticate to Sentinel independently of data-node credentials.
TLS applies to the configured topology. Host/port formatting supports IPv6.

Connect is idempotent after initialization even when a health probe is down.
One pool and one health supervisor belong to a Client. Commands use the initialized
pool and let the native client reconnect; they are not rejected by a stale health
flag. IsConnected is the latest periodic health observation. Close is terminal,
cancels startup/backoff and health checks, and waits for their owned resources.
Create a new Client to restart; Connect after Close returns redis.ErrClosed.
All native options enable ContextTimeoutEnabled. Give operations caller deadlines.

Initial counter creation and its TTL run atomically in one Lua operation; existing
counter TTLs are preserved. Negative counter expirations are rejected. Native()
exposes a borrowed pool, so callers must stop native operations before Close.

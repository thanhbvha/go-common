# WebSocket

Production-oriented WebSocket infrastructure for Go services. The module provides a framework-independent core, bounded event execution, user and room fan-out, structured identity and authorization, connection quotas, graceful shutdown, observability, and optional Redis Pub/Sub or NATS routing for multi-node deployments.

Supported HTTP adapters:

- Standard `net/http`: `websocket/adapter/nethttp`
- Gin: `websocket/adapter/gin`
- Echo: `websocket/adapter/echo`
- Fiber: `websocket/adapter/fiber`

The recommended production path is explicit dependency injection through `core.NewManagerWithConfig`. Package-level global APIs remain available for compatibility and simple examples.

## Capabilities

- Single-node operation without an external broker.
- Multi-node best-effort fan-out through Redis Pub/Sub or Core NATS.
- Multiple physical connections per user for tabs and devices.
- User, room, shard-local, and cross-node fan-out.
- Bounded outbound, inbound, executor, and handler concurrency.
- Per-connection message and byte rate limiting.
- Node-local connection quotas per user, account, and tenant.
- Structured identities, event authorization, room authorization, payload validation, and session expiry.
- FIFO event execution for the same connection with parallel execution across partitions.
- Bounded supervision for handlers that ignore cancellation after timeout.
- Readiness checks covering lifecycle, executor, broker, and accounting state.
- Manager-scoped metrics, event latency histograms, and optional tracing hooks.
- Graceful manager, shard, connection, executor, and owned-broker shutdown.
- Benchmarks, fuzz targets, load scenarios, soak scripts, and profiling support.

## Delivery Model

Redis Pub/Sub and Core NATS are best-effort transports. They do not provide acknowledgement, replay, consumer offsets, offline delivery, or exactly-once semantics. Persist business data before fan-out and let reconnecting clients resynchronize from the system of record.

Use the separate `websocket/durable` contract when delivery must be persistent. Backend implementations such as Redis Streams or NATS JetStream are intentionally separate from `pubsub.Manager`.

Typical architecture:

```text
database or service API = source of truth
websocket core          = live client transport
Redis/NATS pubsub       = best-effort cross-node fan-out
durable.Bus backend     = persistent delivery when required
```

Do not use best-effort WebSocket delivery as the only record for payments, ledgers, audits, critical commands, or guaranteed offline messages.

## Architecture

```mermaid
flowchart LR
    Client[WebSocket client] --> Adapter[net/http, Gin, Echo, or Fiber adapter]
    Adapter --> Auth[Authentication, origin policy, and upgrade limits]
    Auth --> Manager[core.Manager]
    Manager --> Shard[core.Shard]

    subgraph Connection lifecycle
        Reader[read pump] --> Inbound[bounded shard inbound queue]
        Writer[write pump] <-- SendQueue[bounded connection send queue]
    end

    Shard --> Reader
    Shard --> SendQueue
    Inbound --> Registry[EventRegistry validation and authorization]
    Registry --> Executor[partitioned bounded executor]
    Executor --> Supervisor[bounded handler supervisor]
    Supervisor --> Handler[application handler]

    Manager <--> Broker[Noop, Redis Pub/Sub, or NATS]
    Broker <--> Peers[peer application nodes]
```

### Manager

`core.Manager` owns the runtime configuration, shards, connection accounting, event executor, handler supervisor, event registry, authorization policy, observer, and pub/sub registrations.

The manager lifecycle is:

```text
StateStarting -> StateRunning -> StateDraining -> StateStopped
```

Only `StateRunning` accepts new connections.

### Shards and rooms

A shard is a technical connection partition, not a business chat room. User IDs are routed with xxHash:

```text
shard index = xxhash(userID) % MaxShards
```

Shard state uses lock-based synchronous registration, unregistration, room mutation, and fan-out. The inbound event queue remains asynchronous and bounded.

A room is a logical fan-out group inside a shard. One connection may join multiple rooms, subject to `MaxRoomsPerConnection`. Removing a connection only visits the rooms joined by that connection rather than scanning every room in the shard.

### Event execution

Inbound messages use this envelope:

```json
{
  "type": "chat.send",
  "data": {
    "text": "hello"
  }
}
```

Events from the same connection are assigned to the same executor partition and retain FIFO execution order. Events from different connections may execute concurrently. There is no global ordering across connections, shards, processes, or broker nodes.

Each handler also runs through a fixed-capacity supervisor. If a handler exceeds its timeout:

- Its context is canceled.
- The executor partition is released for later work.
- A non-cooperative handler remains counted as quarantined until it exits.
- New work is rejected when the fixed supervisor capacity is exhausted.

This bounds goroutine growth even when application handlers ignore cancellation.

## Installation

```bash
go get github.com/thanhbvha/go-common
```

## Quick Start: Explicit Manager

The following `net/http` example uses an explicit manager, structured identities, a private event registry, authorization, bounded configuration, and graceful shutdown.

```go
package main

import (
    "context"
    "encoding/json"
    "errors"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"

    wshttp "github.com/thanhbvha/go-common/websocket/adapter/nethttp"
    "github.com/thanhbvha/go-common/websocket/core"
)

type authorizer struct{}

func (authorizer) CanSubscribe(ctx context.Context, identity core.Identity, roomID string) error {
    if identity.UserID == "" {
        return core.ErrUnauthorized
    }
    return nil
}

func (authorizer) CanPublish(ctx context.Context, identity core.Identity, eventType string) error {
    if identity.UserID == "" {
        return core.ErrUnauthorized
    }
    return nil
}

func main() {
    registry := core.NewEventRegistry()
    registry.Register("chat.send", core.HandlerConfig{
        Timeout:         2 * time.Second,
        MaxPayloadBytes: 4 * 1024,
        Validate: func(payload []byte) error {
            if !json.Valid(payload) {
                return core.ErrInvalidPayload
            }
            return nil
        },
        Handler: func(ctx context.Context, conn *core.Connection, message core.IncomingMessage) error {
            conn.SendJSON(core.OutgoingMessage{
                Type:      "chat.echo",
                Data:      json.RawMessage(message.Data),
                RequestID: conn.GetRequestID(),
            })
            return nil
        },
    })

    config := core.DefaultManagerConfig()
    config.EventRegistry = registry
    config.Authorizer = authorizer{}
    config.MaxConnections = 20_000
    config.MaxShards = 64
    config.MaxShardConnections = 1_000
    config.MaxConnectionsPerUser = 5
    config.MaxConnectionsPerTenant = 5_000
    config.MaxMessageBytes = 16 * 1024
    config.MessagesPerSecond = 20
    config.MessageBurst = 50

    manager := core.NewManagerWithConfig(config)

    handler := wshttp.NewHandler(wshttp.Config{
        Manager: manager,
        AuthenticateIdentity: func(r *http.Request) (core.Identity, error) {
            userID := r.Header.Get("X-User-ID")
            tenantID := r.Header.Get("X-Tenant-ID")
            if userID == "" {
                return core.Identity{}, errors.New("unauthorized")
            }
            return core.Identity{
                UserID:   userID,
                TenantID: tenantID,
                SessionID: r.Header.Get("X-Session-ID"),
            }, nil
        },
    })

    mux := http.NewServeMux()
    mux.HandleFunc("/ws", handler.HandleUpgrade)
    mux.HandleFunc("/health", handler.HandleHealthCheck)

    server := &http.Server{
        Addr:              ":8080",
        Handler:           mux,
        ReadHeaderTimeout: 5 * time.Second,
    }

    go func() {
        if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
            panic(err)
        }
    }()

    signals := make(chan os.Signal, 1)
    signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
    <-signals

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    _ = server.Shutdown(ctx)
    _ = manager.Shutdown(ctx)
}
```

Use credentials established by trusted middleware or validated bearer/session tokens in real applications. Do not trust identity headers directly from the public internet.

## Compatibility Quick Start

For existing applications, package-level registration and the global manager remain supported:

```go
core.RegisterHandler("chat.send", func(conn *core.Connection, message core.IncomingMessage) error {
    return nil
})

handler := wshttp.NewHandler(wshttp.Config{
    Authenticate: func(r *http.Request) (string, error) {
        return authenticateUser(r)
    },
})

defer core.GetGlobalManager().Shutdown(context.Background())
```

New production code should prefer an explicit `EventRegistry`, `Authorizer`, `Observer`, pub/sub manager, and `*core.Manager` injection. This avoids initialization-order dependencies and permits multiple isolated managers in the same process.

## Adapter Configuration

All adapters expose equivalent concepts with framework-specific callback types:

| Field | Purpose |
|---|---|
| `Manager` | Explicit `*core.Manager`; falls back to `core.GetGlobalManager()` when omitted |
| `AuthenticateIdentity` | Preferred structured authentication callback; takes precedence over `Authenticate` |
| `Authenticate` | Legacy callback returning only `UserID` |
| `RateLimiter` | Limits upgrade attempts; defaults to the package global limiter |
| `ConnectionLimiter` | Limits concurrent upgrades by `ConnectionKey` |
| `ConnectionKey` | Resolves the limiter key and connection metadata key |
| `Upgrader` | Gorilla upgrader for net/http, Gin, and Echo |
| `CheckOrigin` | Fiber browser-origin policy |

Adapter constructors:

```go
netHTTPHandler := wsNetHTTP.NewHandler(wsNetHTTP.Config{Manager: manager, AuthenticateIdentity: authenticateHTTP})
ginHandler := wsGin.NewHandler(wsGin.Config{Manager: manager, AuthenticateIdentity: authenticateGin})
echoHandler := wsEcho.NewHandler(wsEcho.Config{Manager: manager, AuthenticateIdentity: authenticateEcho})
fiberHandler := wsFiber.NewHandler(wsFiber.Config{Manager: manager, AuthenticateIdentity: authenticateFiber})
```

The adapter default authenticator rejects every upgrade. Authentication must be explicitly configured.

## Manager Configuration

Start from `core.DefaultManagerConfig()` and override values based on measured workload characteristics.

### Dependencies and ownership

| Field | Default | Meaning |
|---|---:|---|
| `PubSub` | New no-op manager | Cross-node message router; no-op means single-node mode |
| `PubSubOwnership` | `PubSubBorrowed` | Whether manager shutdown also shuts down the broker manager |
| `EventRegistry` | New registry | Event type to validation, timeout, and handler mapping |
| `Authorizer` | Allow-all | Event publication and room subscription policy |
| `EventExecutor` | Created internally | Optional injected partitioned executor |
| `Observer` | `AtomicObserver` | Manager-scoped metrics and event outcomes |

Use `PubSubBorrowed` when a Redis/NATS manager is shared with another component or manager. Use `PubSubOwned` when the WebSocket manager exclusively owns it.

An injected `EventExecutor` is treated as borrowed and is not shut down by the manager. An internally created executor is owned and shut down by the manager.

### Capacity and queues

| Field | Default | Scope and behavior |
|---|---:|---|
| `MaxConnections` | `200000` | Logical admission ceiling for active plus reserved connections in one manager |
| `MaxShards` | `1000` | Maximum technical shards, not room count |
| `MaxShardConnections` | `100000` | Per-shard connection ceiling |
| `SendQueueSize` | `256` | Outbound messages buffered per connection |
| `InboundQueueSize` | `4096` | Raw client events buffered per shard |
| `BroadcastQueueSize` | `4096` | Compatibility field; synchronous broadcast currently does not use this queue |
| `ExecutorPartitions` | `runtime.NumCPU()` | FIFO worker partitions for business events |
| `ExecutorQueueSize` | `256` | Queued tasks per executor partition |
| `MaxPendingHandlers` | `64` | Pending or running business events per connection |
| `MaxHandlerGoroutines` | `runtime.NumCPU() * 4` | Manager-wide handler and quarantined-handler permit budget |

These values are limits, not validated server capacity. Set production values only after Linux, TLS, kernel, memory, broker, and business-handler load testing.

Approximate executor queue capacity is:

```text
ExecutorPartitions * ExecutorQueueSize
```

One partition may become full while other partitions have capacity because tasks are assigned by connection ID.

### Payload and rate limits

| Field | Default | Meaning |
|---|---:|---|
| `MaxMessageBytes` | `1024` | Maximum inbound WebSocket message size |
| `MessagesPerSecond` | `100` | Sustained inbound messages per second per connection |
| `MessageBurst` | `200` | Maximum message token bucket burst per connection |
| `BytesPerSecond` | `65536` | Sustained inbound bytes per second per connection |
| `ByteBurst` | `131072` | Maximum byte token bucket burst per connection |

Both message and byte limiters must allow an inbound message. A rejected message is dropped rather than queued indefinitely.

The default 1 KiB payload is appropriate for small commands, presence, or typing events. Chat or document patches usually require an explicit larger limit. Large files and media should use HTTP/object storage rather than WebSocket frames.

### Rooms, consumers, and identity quotas

| Field | Default | Meaning |
|---|---:|---|
| `MaxRoomsPerConnection` | `100` | Maximum logical rooms joined by one physical connection |
| `SlowConsumerDropThreshold` | `1` | Consecutive full outbound queue events before closing a connection |
| `MaxConnectionsPerUser` | `0` | Node-local per-user connection quota; zero disables it |
| `MaxConnectionsPerAccount` | `0` | Node-local per-account connection quota; zero disables it |
| `MaxConnectionsPerTenant` | `0` | Node-local per-tenant connection quota; zero disables it |

A successful outbound enqueue resets the slow-consumer consecutive-drop count. Raising the threshold tolerates brief congestion but does not restore already dropped messages.

Identity quotas are enforced atomically with manager capacity reservation. They are node-local, not strict cluster-wide quotas. With ten pods, a per-tenant limit of 1,000 may permit approximately 10,000 cluster-wide connections depending on load distribution.

### Network deadlines and logging

| Field | Default | Meaning |
|---|---:|---|
| `WriteWait` | `10s` | Deadline for an outbound frame or ping write |
| `PongWait` | `60s` | Read deadline extended by each pong |
| `PingPeriod` | `54s` | Ping interval; must remain below `PongWait` |
| `DisableLifecycleLogging` | `false` | Disables high-volume connection open/close logs; errors remain enabled |

Lifecycle logs are rate-limited. Operational metrics should be the authoritative source for connection counts and drops.

## Event Registration

### Configured registry

```go
registry := core.NewEventRegistry()

registry.Register("document.patch", core.HandlerConfig{
    MaxPayloadBytes: 32 * 1024,
    Timeout:         1500 * time.Millisecond,
    Validate: func(payload []byte) error {
        if len(payload) == 0 {
            return core.ErrInvalidPayload
        }
        return nil
    },
    Handler: func(ctx context.Context, conn *core.Connection, message core.IncomingMessage) error {
        return applyPatch(ctx, conn.Identity(), message.Data)
    },
})

config.EventRegistry = registry
```

### Compatibility registration

```go
core.RegisterHandler("chat.send", func(conn *core.Connection, message core.IncomingMessage) error {
    return nil
})

core.RegisterContextHandler("chat.send", 2*time.Second, func(
    ctx context.Context,
    conn *core.Connection,
    message core.IncomingMessage,
) error {
    return nil
})
```

Handlers must observe `ctx` in database, HTTP, queue, and blocking calls. Timeout releases the executor partition, but Go cannot forcibly terminate a non-cooperative goroutine. The supervisor bounds those remaining goroutines with `MaxHandlerGoroutines`.

## Identity and Authorization

```go
type Identity struct {
    UserID    string
    TenantID  string
    AccountID string
    SessionID string
    ExpiresAt time.Time
}
```

`UserID` is required by the standard adapters. `TenantID`, `AccountID`, and `SessionID` enable multi-tenant authorization, quotas, and revocation. A non-zero `ExpiresAt` automatically closes the connection when the identity expires.

Authorization interface:

```go
type Authorizer interface {
    CanSubscribe(context.Context, Identity, string) error
    CanPublish(context.Context, Identity, string) error
}
```

The default authorizer allows every operation. Applications with private rooms or restricted event types must inject an authorizer.

Revoke active sessions:

```go
count := manager.DisconnectIdentity(func(identity core.Identity) bool {
    return identity.SessionID == revokedSessionID
}, "session revoked")
```

## User and Room Operations

Inside an event handler:

```go
shard := conn.GetShard()

if err := shard.JoinRoom(ctx, "project:42", conn); err != nil {
    return err
}

shard.SendToUser("user-123", []byte(`{"type":"notification"}`))
shard.SendToRoom("project:42", []byte(`{"type":"project.updated"}`))
shard.Broadcast([]byte(`{"type":"local.broadcast"}`))
shard.BroadcastGlobal([]byte(`{"type":"cluster.broadcast"}`))

if err := shard.LeaveRoom(ctx, "project:42", conn); err != nil {
    return err
}
```

Room join and leave are idempotent. Joining requires the connection to be active on the shard and authorized by `Authorizer.CanSubscribe`.

## Message and Ownership Contract

- One outbound application message is one WebSocket text frame.
- `Connection.Send` clones the caller's byte slice before enqueueing; the caller may immediately reuse or mutate it.
- User, room, and shard fan-out clone a payload once and share the immutable copy across connection queues.
- Successfully enqueued messages are written FIFO on that connection.
- No global ordering is promised across connections, shards, processes, or broker nodes.
- A full send queue drops the current message and may close the slow consumer according to `SlowConsumerDropThreshold`.
- Inbound queue or executor overload drops/rejects work instead of growing memory without a bound.
- Applications requiring retries must implement idempotency and retry at the business layer or use durable delivery.

## Single-Node Mode

Omit `PubSub` or explicitly use a no-op manager:

```go
config := core.DefaultManagerConfig()
config.PubSub = pubsub.NewNoopManager()
config.PubSubOwnership = core.PubSubOwned

manager := core.NewManagerWithConfig(config)
```

Single-node mode supports all local connection, user, room, executor, quota, authorization, and observability features. Cross-node messages are unavailable.

## Redis Pub/Sub

```go
import (
    "context"
    "time"

    commonredis "github.com/thanhbvha/go-common/redis"
    "github.com/thanhbvha/go-common/websocket/core"
    redispubsub "github.com/thanhbvha/go-common/websocket/pubsub/redis"
)

redisConfig := commonredis.DefaultConfig()
redisConfig.Host = "redis"
redisConfig.Port = 6379

redisClient := commonredis.New(redisConfig)
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := redisClient.Connect(ctx); err != nil {
    return err
}

bus := redispubsub.NewManagerWithClient(redisClient)

config := core.DefaultManagerConfig()
config.PubSub = bus
config.PubSubOwnership = core.PubSubOwned
manager := core.NewManagerWithConfig(config)
```

Use `PubSubBorrowed` if another component owns `bus`.

## NATS

```go
import (
    commonnats "github.com/thanhbvha/go-common/nats"
    "github.com/thanhbvha/go-common/websocket/core"
    natspubsub "github.com/thanhbvha/go-common/websocket/pubsub/nats"
)

natsConfig := commonnats.DefaultConfig()
natsConfig.URLs = []string{"nats://nats:4222"}
natsClient := commonnats.New(natsConfig)
if err := natsClient.Connect(ctx); err != nil {
    return err
}

bus := natspubsub.NewManagerWithClient(natsClient)

config := core.DefaultManagerConfig()
config.PubSub = bus
config.PubSubOwnership = core.PubSubOwned
manager := core.NewManagerWithConfig(config)
```

Refer to the `nats` package configuration for URL, credentials, timeout, and reconnect settings.

## Broker Failure Policy and Readiness

Redis and NATS managers support these policies:

| Policy | Behavior |
|---|---|
| `pubsub.FailStartup` | Broker operations fail and readiness remains false when unavailable; the application must enforce startup failure with `WaitReady` |
| `pubsub.StayUnready` | Process may remain alive but broker readiness is false until recovery |
| `pubsub.AllowStandalone` | Permit standalone operation when no broker is available |

Broker managers track desired and active subscriptions, reconcile missing subscriptions with capped jittered backoff, expose reconnect state and counters, and implement `WaitReady(ctx)`.

Constructors return a manager rather than a startup error. If broker availability is mandatory, use `NewManagerWithClientAndPolicy(..., pubsub.FailStartup)`, call `WaitReady` with a startup deadline, and terminate startup when it returns an error.

```go
if readyProvider, ok := config.PubSub.(pubsub.WaitReadyProvider); ok {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    if err := readyProvider.WaitReady(ctx); err != nil {
        return err
    }
}
```

Broker states include `standalone`, `disconnected`, `recovering_subscriptions`, `ready`, and `closed`.

## Durable Delivery Contract

Persistent backends implement:

```go
type Bus interface {
    Publish(context.Context, string, []byte, PublishOptions) (Message, error)
    Consume(context.Context, string, ConsumerOptions) (Consumer, error)
    WaitReady(context.Context) error
    Close() error
}
```

Consumers support `Receive`, `Ack`, `Nack`, and `Close`. `PublishOptions` includes a deduplication ID; `ConsumerOptions` includes name, batch size, and acknowledgement timeout.

The package currently defines the contract. A Redis Streams or JetStream implementation must be supplied separately.

## Observability

`AtomicObserver` records manager-local cumulative counters:

```text
connections opened and closed
inbound messages and bytes
outbound messages and bytes
outbound drops and slow consumers
malformed, unknown, submitted, dropped, errored, timed-out, and panicked events
```

Event latency buckets:

```text
< 1 ms
< 5 ms
< 10 ms
< 50 ms
< 100 ms
>= 100 ms
```

Custom observers implement `core.Observer`. Snapshot-capable implementations may also implement `core.SnapshotObserver`. Event latency and tracing are optional through `core.LatencyObserver` and `core.TraceObserver`.

Avoid raw user, account, tenant, room, or session IDs as metric labels because they create unbounded cardinality.

Manager stats include:

- Manager lifecycle and configured limits.
- Total and per-shard connections.
- Executor queue, completion, rejection, panic, and in-flight state.
- Handler supervisor running, quarantined, rejected, and accepting state.
- Runtime and event counters.
- Event latency buckets.

## Health and Operational Endpoints

Every adapter provides equivalents of:

- `HandleHealthCheck`
- `HandleStats`
- `HandleShardManagement`

Readiness requires:

```text
manager state is running
AND event executor is accepting
AND broker is ready
AND manager and shard connection accounting match
```

Operational endpoints expose topology and runtime metadata. Put them on an internal listener or protect them with administrator authentication. The Fiber server helper only registers public routes documented by that helper; register sensitive operational handlers explicitly.

## Graceful Shutdown

Shut down HTTP admission first, then the WebSocket manager:

```go
ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
defer cancel()

_ = httpServer.Shutdown(ctx)
if err := manager.Shutdown(ctx); err != nil {
    return err
}
```

Manager shutdown:

- Moves the manager to draining state and rejects new connections.
- Shuts down all shards and closes active connections.
- Waits for started connection pumps within the supplied context.
- Stops handler supervision from accepting new work.
- Shuts down an internally owned event executor.
- Unregisters only this manager's pub/sub callbacks.
- Shuts down pub/sub only when configured as `PubSubOwned`.

`Connection.Close` is idempotent and non-blocking. `Connection.Wait(ctx)` and `Connection.CloseAndWait(ctx, reason)` provide bounded lifecycle waiting after a connection has started.

## Security Checklist

- Configure `AuthenticateIdentity` or `Authenticate`; the default rejects upgrades.
- Derive identity from validated credentials or trusted middleware, not arbitrary query parameters or public headers.
- Inject an `Authorizer` for private events and rooms; the default is allow-all.
- Keep browser same-origin defaults or configure a strict origin allowlist.
- Configure trusted proxies before using forwarded client IP headers.
- Set realistic message, byte, payload, room, connection, account, and tenant limits.
- Keep administrative health, stats, and shard endpoints private or authenticated.
- Never log frames, credentials, tokens, or broker payload contents.
- Persist critical data before best-effort fan-out.
- Use idempotency and durable delivery for operations that cannot be lost or duplicated.

## Error and Overload Behavior

| Condition | Behavior |
|---|---|
| Manager is draining or stopped | Reject upgrade or connection registration |
| Manager or identity quota reached | Reject and close the new connection |
| Shard capacity reached | Reject and close before manager accounting commits |
| Inbound rate limit exceeded | Drop the inbound event |
| Shard inbound queue full | Drop the inbound event |
| Per-connection pending-handler limit reached | Drop the event |
| Executor partition queue full | Reject and count the event as dropped |
| Handler supervisor capacity exhausted | Reject the invocation |
| Handler timeout | Cancel context, release executor partition, quarantine if still running |
| Outbound queue full | Drop message and close after configured consecutive threshold |
| Redis/NATS unavailable | Behavior follows cluster failure policy and readiness state |
| Handler panic | Recover, count panic, keep executor available for future tasks |

## Performance and Capacity

The repository includes logging-free microbenchmarks, fuzz tests, integration tests, load scenarios, soak scripts, and a profiling helper:

```bash
go test -run '^$' -bench . -benchmem ./websocket/core
go test -run '^$' -bench BenchmarkPubSubDecode -benchmem ./websocket/pubsub
bash scripts/websocket-profile.sh
```

Load generator examples:

```bash
go run ./examples/websocket/loadtest \
  -target ws://127.0.0.1:8080/ws \
  -scenario message \
  -connections 1000 \
  -messages 100

go run ./examples/websocket/loadtest -scenario idle -connections 10000 -hold 10m
go run ./examples/websocket/loadtest -scenario churn -connections 1000 -reconnects 10
go run ./examples/websocket/loadtest -scenario reconnect -connections 1000 -reconnects 5
go run ./examples/websocket/loadtest -scenario rolling-drain -connections 5000 -hold 5m
```

Available scenarios are `message`, `idle`, `churn`, `fanout`, `reconnect`, and `rolling-drain`.

Microbenchmarks measure in-memory operations. They do not include real network writes, TLS, kernel socket buffers, broker latency, database calls, downstream services, or production scheduling. Do not derive maximum connection capacity directly from benchmark nanoseconds.

Before production approval, retain evidence for:

- Linux and TLS capacity at target container limits.
- Payload, message-rate, and fan-out distributions representative of production.
- 24-hour and 72-hour soak tests.
- Multi-node and multi-zone behavior.
- Broker restart and subscription recovery.
- Reconnect storms.
- Rolling drain and canary rollback.
- CPU, memory, file descriptor, network, queue, drop, and latency thresholds.

## Recommended Starting Profiles

These are initial hypotheses, not production guarantees.

### Chat or notification service

```go
config := core.DefaultManagerConfig()
config.MaxConnections = 20_000
config.MaxShards = 64
config.MaxShardConnections = 1_000
config.SendQueueSize = 128
config.InboundQueueSize = 1_024
config.MaxMessageBytes = 16 * 1024
config.MessagesPerSecond = 20
config.MessageBurst = 50
config.BytesPerSecond = 32 * 1024
config.ByteBurst = 64 * 1024
config.MaxPendingHandlers = 16
config.MaxHandlerGoroutines = 64
config.MaxRoomsPerConnection = 50
config.SlowConsumerDropThreshold = 2
config.MaxConnectionsPerUser = 5
```

### Presence or telemetry with many idle connections

```go
config := core.DefaultManagerConfig()
config.MaxConnections = 50_000
config.MaxShards = 128
config.MaxShardConnections = 1_000
config.SendQueueSize = 32
config.InboundQueueSize = 512
config.MaxMessageBytes = 4 * 1024
config.MessagesPerSecond = 5
config.MessageBurst = 10
config.BytesPerSecond = 8 * 1024
config.ByteBurst = 16 * 1024
config.MaxPendingHandlers = 4
config.MaxHandlerGoroutines = 32
config.MaxRoomsPerConnection = 10
config.SlowConsumerDropThreshold = 1
```

Queue size has direct memory consequences. A `SendQueueSize` of 256 reserves channel slots for every connection even when idle. Smaller queues are usually preferable for large idle populations.

## Suitable Workloads

Strong fits:

- Chat with database persistence and reconnect synchronization.
- Notifications, progress updates, and operational alerts.
- Presence, typing indicators, cursors, and ephemeral state.
- Realtime dashboards and device monitoring.
- Multi-tenant SaaS live updates.
- Live tracking and game lobbies.
- Turn-based or moderate-rate realtime commands.

Requires additional domain infrastructure:

- Collaborative editing needs OT or CRDT, conflict handling, and replay.
- Financial feeds need sequence numbers, snapshots, and resume semantics.
- Critical workflows need durable storage, idempotency, acknowledgements, and retries.
- Strict cluster-wide quotas need a distributed lease or reservation system.
- High-frequency authoritative game simulation needs workload-specific networking and state architecture.

Not intended for direct file, audio, or video streaming.

## Testing

```bash
go test ./websocket/...
go test -race ./websocket/core ./websocket/pubsub/... ./websocket/adapter/...
go vet ./...
```

Redis and NATS integration tests use the `integration` build tag and environment gates documented in the repository CI and operational files.

Fuzz regression corpora under `websocket/core/testdata/fuzz` are source-controlled test fixtures. They preserve inputs that previously exposed parser failures and should not be ignored or deleted as build artifacts.

## Key Types

| Symbol | Description |
|---|---|
| `core.ManagerConfig` | Dependencies, limits, queues, quotas, and deadlines for one manager |
| `core.Manager` | Lifecycle, shard routing, accounting, pub/sub integration, and shutdown |
| `core.Shard` | Connection, user-session, room, and fan-out partition |
| `core.Connection` | Active client session with read/write pumps and bounded send queue |
| `core.Conn` | Minimal framework-independent transport abstraction |
| `core.EventRegistry` | Manager-scoped event registration table |
| `core.HandlerConfig` | Handler timeout, validation, and payload policy |
| `core.Identity` | User, tenant, account, session, and expiry metadata |
| `core.Authorizer` | Event and room authorization policy |
| `core.Observer` | Runtime and event observation interface |
| `core.AtomicObserver` | Default manager-local atomic metrics and latency buckets |
| `pool.PartitionedExecutor` | Bounded FIFO-per-partition business executor |
| `pubsub.Manager` | Best-effort cross-node routing abstraction |
| `redispubsub.Manager` | Redis Pub/Sub backend |
| `natspubsub.Manager` | Core NATS backend |
| `durable.Bus` | Persistent delivery contract for separate backend implementations |

## Further Documentation

- `examples/websocket`: runnable net/http, Fiber, Gin, and Echo examples.
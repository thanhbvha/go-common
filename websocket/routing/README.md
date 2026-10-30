# Targeted realtime routing

This is an opt-in layer. Existing WebSocket constructors, text-frame envelopes,
local shard APIs, and legacy pub/sub remain available.

A `Gateway` owns tenant-scoped local user/session and room indexes. A `Router`
resolves Redis session leases and groups delivery by gateway. NATS Core and
Redis Pub/Sub implement the same request/result contract. Neither transport
stores offline notifications.

## Wiring

```go
registry := redisrouting.NewRegistry(redisClient, "my-app")
delivery := redisrouting.NewTransport(redisClient, "my-app")
// Alternative: natsrouting.NewTransport(natsConn, "my-app")

gateway, err := routing.NewGateway(routing.GatewayConfig{
    Registry: registry,
    Transport: delivery,
})
// Check err, then start with a startup deadline.
err = gateway.Start(startupCtx)

config := core.DefaultManagerConfig()
config.Distributed = gateway
manager := core.NewManagerWithConfig(config)

router, err := routing.NewRouter(routing.RouterConfig{
    Registry: registry,
    Transport: delivery,
})
result, err := router.SendToUser(ctx, "tenant", "user", payload, stableMessageID)
// Check err and result.Retryable(). result.Count(routing.Enqueued) counts sessions.
```

Imports: `websocket/core`, `websocket/routing`,
`websocket/routing/redis` (alias `redisrouting`), and optionally
`websocket/routing/nats` (alias `natsrouting`). Check every returned error.

Start a router-only transport with `Start(ctx, routing.NewID(), nil)`. A transport
belongs to one process incarnation and has two fixed subscriptions, independent
of connection count. Node IDs default to UUIDs; custom IDs must be unique per
incarnation, including across rolling restarts.

Setting `ManagerConfig.Distributed` substitutes a noop legacy pub/sub manager.
Use the Router API for cross-node traffic; do not also call legacy publication
methods for the same message. Legacy local send APIs retain their existing,
shard-scoped behavior. New APIs always require tenant identity.

`AuthenticateIdentity` must supply tenant/user and may supply a stable session
ID. With no session ID, the manager generates one per physical connection.
Reusing a session ID replaces that logical session's location and creates a
new fencing token. Other session IDs remain independent devices/tabs.

## Guarantees and limits

- `Enqueued` means accepted into the gateway queue. Socket writes and client
  receipt can still fail afterward. `Connection.Enqueue` returns distinct
  `ErrConnectionClosed` and `ErrSendQueueFull`; `Send` is true only on acceptance.
- Rooms resolve node membership and deliver across all matching local shards.
  Room results contain recipient `Counts` and compact node/status outcomes,
  avoiding a response proportional to the number of room members.
- Default routing deadline: 5 seconds. Stale/not-found locations get at most two
  re-resolves. Unknown results and queue-full are returned to the caller;
  EventBus bridge redelivery uses EventBus's configured retry policy.
- A missing result is `Unknown`, even if publication succeeded. Reuse the same
  message ID when retrying. Gateway dedupe lasts 1 minute and at most 100,000
  entries by default, using constant-time bounded eviction. Capacity eviction,
  restart, and retries beyond this window may produce duplicates.
- No client ACK, offline inbox/replay, exactly-once delivery, or global ordering.
  FIFO remains the order of successful enqueue on each connection. There is
  no automatic cluster broadcast fallback.
- Gateway lease TTL defaults to 30 seconds; renewal runs every 10 seconds.
  Sessions and rooms renew in bounded pipelines. Expired nodes disappear from
  resolution. An expired gateway stops admission/delivery until reconciliation
  succeeds. A superseded session is closed when fencing is detected on renewal;
  cross-node takeover is therefore bounded by the lease/renewal window.
- The Redis registry defaults to 1,000 sessions per tenant/user; configure
  `redisrouting.WithMaxSessionsPerUser` alongside application quotas. Individual
  expired session records are pruned even when another device keeps the user
  hash alive. Short leases do not shorten another session/room member's TTL.
- Redis keys used together in Lua share a tenant hash slot, supporting
  `redis.UniversalClient`. One tenant occupies one slot; measure hot-tenant
  workloads before choosing Redis Cluster placement.
- Each routing call has at most 32 concurrent destination workers. Transport
  permits at most 1,024 pending requests and bounds encoded messages to 2 MiB.
  Raw payload default is 1 MiB; broker limits must accommodate JSON/base64 and
  envelope overhead (configure a lower payload limit where necessary).
- Gateway/transport borrow broker clients; manager borrows gateway. Shutdown:
  stop HTTP admission, drain business intake as appropriate, shut down manager,
  close gateway/router transport, then close broker clients. Each cleanup
  should receive a fresh bounded context. Closing a gateway alone stops routing;
  the connection manager remains responsible for closing sockets.

`Gateway.Stats` reports session/room/dedupe counts, lease failures, accepted and
rejected enqueues. `Router.Stats` reports attempts, unknowns and partial results.
Counters do not label user or session IDs. Legacy pub/sub stats now expose
bounded executor dispatch counters; manager stats expose subscription errors.

## Business bridge

`websocket/integration/eventbus.New` declares `ws.command` and
`ws.notification` before `Bus.Start`. A configured Router subscribes with the
shared group name `realtime-router`; multiple instances compete in that group.
Pass the existing EventBus driver's `WithEventLayoutTargeted` option for each
Event topology. This option controls subscriber retry layout, not gateway selection.

`HandlerConfig{Dispatch: core.Business}` publishes validated/authorized client
messages via `ManagerConfig.CommandPublisher`. Identity comes from the
connection, never from the client payload. Local is the default dispatch policy.
Once EventBus accepts a task, disconnecting the client does not cancel it.

Custom business events use `bridge.Mapping` with an explicit mapper to a
`bridge.Notification`. Mapping errors and invalid targets are permanent errors;
unknown delivery and queue-full return errors for EventBus retry/DLQ. Offline
is terminal success. The bridge never starts or closes the shared bus. Database
atomicity and outbox/inbox behavior belong to the application.

See the runnable [cluster example](../../examples/websocket/cluster/README.md).

## Migration

1. Deploy core fixes and observe subscription/dispatch/queue counters.
2. Deploy gateway instances with unique incarnation IDs, Redis registry and
   the selected targeted transport. Verify lease and broker readiness.
3. Deploy the shared realtime-router group and business workers.
4. Cut over producers to the new APIs by application configuration, with one
   publication path per message. Route a test tenant first.
5. Roll back producers and gateway configuration together to legacy mode;
   drain outstanding work first to avoid duplicate/lost in-flight notifications.

Shard creation now returns nil at capacity instead of silently choosing the
default shard. Existing shards remain accessible at the limit. Capacity counts
include the default shard; callers must handle rejection.

For a realtime-oriented EventBus profile, the example explicitly uses batch size
64 with Redis `WithBlock(25*time.Millisecond)` or NATS
`WithFetchTimeout(25*time.Millisecond)`. Shorter polls cost more idle broker work
but avoid long waits while alternating fresh/retry lanes. Tune this tradeoff
alongside concurrency/prefetch; defaults intended for general background work
are not a realtime latency target. See [local measurements](../BENCHMARKS.md).

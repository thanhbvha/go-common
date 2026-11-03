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
rejected enqueues, plus `UnregisterFailures` (failed cleanup attempts, including
canceled attempts) and `UnregisterRetries` (retries actually started).
`Router.Stats` reports attempts, unknowns and partial results.
Counters do not label user or session IDs. Legacy pub/sub stats now expose
bounded executor dispatch counters; manager stats expose subscription errors.

## Session lifecycle and failure recovery

Lifecycle operations are ordered per logical session rather than per gateway.
Waiting for a session gate or registry permit observes the caller's deadline.
`GatewayConfig.RegistryConcurrency` defaults to 32 and bounds registry operations.
Remove detaches local indexes before waiting for registry work, even if its
budget has expired. Renewal processes at most 256 idle sessions per batch,
skipping busy sessions; a session with an expired local lease rejects delivery.
Per-session gates remain held across renew and room refresh so a delayed renewal
cannot race a successful remove/leave and recreate its metadata. Replacement and
fencing close callbacks run after releasing those gates.

Delivery snapshots recipients under the index mutex, then performs enqueue outside
it. A separate cancellable gate preserves enqueue order and dedupe per session.
The bounded dedupe cache uses up to 64 stripes with capacity divided between them;
hash skew can evict entries before the aggregate capacity is reached. No callback
or socket close runs under the shared index/cache mutexes. Slow-consumer closure
marks the connection closed immediately and schedules at most one network closer.

`Gateway.Close` atomically closes operation admission, cancels in-flight operation
contexts, joins those operations and the renewal loop, then closes transport.
A registration response received after cancellation cannot commit locally; the
gateway attempts generation-fenced cleanup with a fresh five-second budget.
Close may time out if a registry/callback ignores cancellation; retry Close with a
fresh context before releasing borrowed clients. NATS subscription cancellation
joins running callbacks, and RPC Close tracks callbacks independently as well.
After Close, Remove can still detach local indexes but returns unavailable rather
than starting registry I/O; shut down managers before closing their borrowed gateway.

A replacement is locally routable while registration is in flight. The old
connection is closed only after registry confirmation. On registration error,
its local session and room indexes are restored. An ambiguous network error may
still mean Redis accepted the new generation; later renewal fences the old owner
if necessary. This is not a transactional rollback of a remote registration.

Unregister detaches local indexes before broker work and attempts cleanup at most
three times, with 10 ms and 20 ms backoffs within the supplied context. Invalid,
stale and context errors are not retried. Persistent failure is returned and
counted; lease expiry remains the final recovery path. There is no unbounded
background retry queue. Redis implements optional `SessionRemover` to remove a
session generation and its known room memberships in one tenant-slot Lua call.
Delayed cleanup removes only the old generation's room members. Direct registry
`Remove` retains its session-only contract; use `RemoveSession` with known rooms
when room cleanup is also required.

Redis implements optional `SessionLeaseRenewer.RenewBatch`, returning a result for
every input and continuing beyond stale sessions across 256-session chunks.
Gateway fences only stale owners and refreshes rooms only for renewed sessions.
Other failures prevent extension of gateway readiness. Custom registries can
continue implementing only `SessionRegistry`; the legacy stale-result path uses
resolve for fencing and waits for a successful renewal before extending readiness.

Manager shutdown rejects upgrades and starts shard shutdown concurrently. Shards
detach their maps, stop outbound admission, and flush accepted messages in FIFO
order before closing sockets. New inbound business intake is discarded once the
connection starts draining. `ManagerConfig.DrainTimeout` defaults to five seconds
for outbound drain; the first Shutdown caller's cancellation/deadline also applies.
The manager shares that drain budget across shards. Connection.Drain also handles
the registration window before Run has started the pumps.

Expired drain budgets force socket close. Shutdown retains drain errors, including
`ErrDrainIncomplete` when accepted messages could not be written; another Shutdown
call does not erase a prior forced-drain result. Optional `DrainObserver` reports
connections drained, forced drains and messages still unwritten at the cutoff;
`AtomicObserver.RuntimeMetrics` exposes `connectionsDrained`, `drainForced` and
`drainDiscarded`. This is a socket-write guarantee within the budget, not client
ACK or business-handler completion. Drain application business intake separately
before manager shutdown when handlers need to publish their final responses.

Distributed cleanup runs after outbound drain, with up to 32 workers per shard
and at most five seconds of broker work within the remaining shutdown context.
Every captured session is detached from the gateway even after that budget expires.
The shard owns cleanup for committed connections; the manager only cleans up
registrations that never committed to a shard. Clients must honor context deadlines.
Remote registry cleanup remains best-effort, with lease expiry as outage fallback.

Broadcast fallback and client ACK/offline inbox/replay remain separate feature
work requiring application policy. The EventBus delivery guarantees above are
unchanged. The session hash/expiry data model is unchanged; benchmark large-user
workloads before introducing a different expiry index.

## Registry lookup and registration cost

Single-session `Resolve` uses Lua `HGET`, checks that field's lease, and deletes
only that field if expired. A missing session returns immediately. User-wide
resolution still scans and prunes all fields because it returns every live session.

`Register` replaces a live session directly. New admissions below the physical
hash limit also avoid a scan; when the hash is full, registration prunes expired
fields atomically before enforcing capacity. An expired same-ID field is removed
before admission, so it cannot bypass the limit. Live replacements retain their
existing behavior when a configured limit has been lowered.

Expiry pruning is demand-driven: unrelated expired records can remain until
user-wide resolution, capacity admission, direct lookup/removal, or hash TTL expiry.
They are never returned as live sessions. The physical hash remains bounded by
the configured limit for new admissions; replacing existing fields does not grow
it. No per-user sorted-set index or additional renewal write is introduced.
Existing hash keys, JSON records and generation-fenced renew/remove remain
compatible with older writers and require no migration.

Full-capacity rejection and user-wide resolution retain O(N) scan costs. See the
benchmark reports linked from [WebSocket measurements](../BENCHMARKS.md) for the
measured tradeoffs and saturation-test limits.

## Room lookup and node index rollout

All new Redis registry writers maintain the existing room member sorted set plus
a room-node lease index and per-node member sets in the same tenant hash slot.
Join/renew/remove/leave update these structures atomically. A node's score is the
maximum live member expiry, so removing one device or an older generation cannot
remove another live device's node lease. All derived keys have lease-based TTLs.

The default compatibility reader aggregates distinct node IDs inside Redis and
returns only those IDs. It still scans legacy memberships on the server, but
network response size and Go decoding scale with hosting nodes.

After all writers support the new index, enable the fast reader explicitly:

```go
registry := redisrouting.NewRegistry(client, namespace, redisrouting.WithRoomNodeIndex())
```

The fast reader prunes expired node scores and reads only node leases; it never
loads the recipient member set. Before enabling it on an existing namespace:
upgrade every writer, stop old writers, and wait for the longest old room lease
to expire or for all live memberships to be refreshed by upgraded writers.
An old writer can maintain a legacy membership without updating its node lease.
Keep compatibility reads while writers are mixed. To roll back writers, first
disable indexed reads everywhere. No destructive backfill or key-format rewrite
is needed; the session hash format and legacy room members are preserved.

The node index adds Redis writes and per-room/per-node metadata. Benchmark room
cardinality, membership churn and renewal cost on a disposable real Redis before
enabling it for a workload. One tenant still shares one Redis Cluster slot.

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


## Independent EventBus and delivery brokers

The bridge borrows `eventbus.Bus`, while `routing.Router` uses a separate
`DeliveryTransport`. A Kafka EventBus can therefore feed Redis Pub/Sub or NATS
Core gateway delivery; the Redis session/room registry is still required.
Kafka support belongs to EventBus and does not require a Kafka WebSocket pub/sub
backend. The cluster example exposes `-eventbus-driver` and `-delivery-driver`.
NATS delivery alone does not require JetStream.

`bridge.Config.PublicationDedupe` defaults to `bridge.DedupeRequired`. Built-in
buses expose optional `eventbus.CapabilityProvider` for startup validation;
unsupported required dedupe returns `eventbus.ErrDedupeUnavailable`. Custom bus
wrappers that hide this optional interface retain publication-time validation.

For Kafka, explicitly choose `bridge.DedupeDisabled` and pass
`kafkadriver.WithEventLayoutTargeted()` in `NotificationOptions` and applicable
mappings. This omits producer dedupe reservations while keeping stable message
IDs. Gateway dedupe is unchanged and remains bounded/process-local. Application
handlers must handle duplicate publications; durable business idempotency and
outbox/inbox behavior are separate concerns. The bridge never silently falls
back after a failed deduplicated publication.

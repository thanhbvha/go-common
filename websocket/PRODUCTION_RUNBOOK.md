# WebSocket production runbook

Target (see the refactor plan, Q4): about 1,000 concurrent connections, load-tested to 5,000 per node, two application nodes plus one Redis (with replica) or one NATS.
Numbers below marked *(estimate)* are reasoning from the code, not measurements; Phase 4 soak replaces them.

## 1. Topology and rules

- Cluster fan-out must use `routing.Gateway` + `routing.Router` (`ManagerConfig.Distributed`). The legacy `PubSub` path is single-node/dev only and `ValidateProduction` rejects it otherwise.
- Build the manager with `core.NewProductionManager(cfg)`. It refuses a missing authorizer, a `PubSub` without `Distributed`, and a cluster transport that does not sign broker messages (`pubsub.AuthRequired`).
- Broker trust: per-node ACL user + TLS with verification on. HMAC signing is the second layer. Setting `AllowUnauthenticatedBroker` is only valid when ACL + TLS/mTLS already make the broker the trust boundary.

## 2. Sizing

| Item | Guidance |
|---|---|
| File descriptors | One per connection plus broker/DB sockets. Set `ulimit -n` (or systemd `LimitNOFILE`) to at least 4x the connection target; 65536 for a 5k node. |
| Memory | Measured about 17.5 KB heap and 4.4 goroutines per idle connection (10,000 connections, TLS proxy). Plan 32 KB/conn for traffic and queues. |
| Goroutines | About 4 per connection. Alert on growth without a matching connection count. |
| Message limits | Chat: `MaxMessageBytes` 4096, `MessagesPerSecond` 10, `MessageBurst` 20. Defaults are 1 KB and 100/s; tune deliberately. |
| Handler capacity | `MaxHandlerGoroutines` defaults to `NumCPU*4`, shared by the node. Handlers must honour their `ctx`: a handler that ignores it keeps its permit and is counted as quarantined. |
| Redis legacy pubsub | One Redis connection per shard channel, up to `MaxShards`+1. Set `MaxShards` to 64–128 if you use it. |

## 3. Load balancer and proxy

- Allow WebSocket upgrade; idle timeout must exceed `PongWait` (60 s default). Use at least 120 s, or the proxy cuts healthy idle sockets and clients see 1006.
- Do **not** trust `X-Forwarded-For` for admission. The adapters key rate limits on the direct peer IP; behind a proxy that makes every client share the proxy IP. Provide a verified `ConnectionKey` and size `IPRateLimiter` for it.
- Pass tokens in headers or a short-lived ticket, not the query string, which lands in proxy logs.
- Origin policy belongs to the `Upgrader.CheckOrigin`; the default is same-origin. Cross-origin must be explicit.
- Honour `Retry-After` on 429/503; clients follow [CLIENT_PROTOCOL.md](CLIENT_PROTOCOL.md).

## 4. Endpoints

| Path | Audience | Use |
|---|---|---|
| `/livez` | orchestrator liveness | Always 200 while the process answers. Never depends on brokers or draining. |
| `/readyz` (`/health`) | load balancer readiness | 200/503 only, no dependency detail. Fails while draining or when executor/broker is unavailable. |
| `HandleStats`, `HandleShardManagement` | operators | Require `Config.AdminToken` (`Authorization: Bearer`). Empty token means always 401. Put them on an internal listener as well. |

Never point liveness at readiness: a broker outage would restart every healthy node and drop all sockets.

## 5. Metrics

`websocket/metrics.Register(meter, metrics.Sources{Manager, Gateway, Router})` exports OpenTelemetry instruments; the application owns the provider and exporter (OTLP or a Prometheus bridge). Collection reads `Manager.Snapshot()` on demand: no sampling goroutine.

Suggested alerts:

| Signal | Condition |
|---|---|
| `websocket.ready` | 0 for more than 30 s on a node that is not draining |
| `websocket.slow_consumers` | rate rising together with `websocket.drops{reason="outbound_queue"}` |
| `websocket.handlers.quarantined` | any sustained increase: a handler ignores its context |
| `websocket.drops{reason="handler_capacity"}` | non-zero: handler permits exhausted |
| `websocket.gateway.lease_failures` | non-zero rate: registry slow or down; node can drop to not-ready |
| `websocket.router.unknown` | rising: deliveries with unknown outcome |
| `websocket.pubsub.subscription_errors` | non-zero (legacy path) |
| `websocket.connections` | vs. `MaxConnections`; and vs. file-descriptor limit |
| `websocket.drain.forced` | non-zero after a deploy: drain budget too short |

Labels are fixed enums. Never add user, tenant, room or IP labels, and do not log credentials or payloads.

## 6. Rollout and drain

Order for stopping a node (matches README "Shutdown and drain"):

1. Fail readiness (`Manager` state leaves `Running`) so the LB stops sending new upgrades.
2. Drain business work while routing and sockets still work.
3. `Manager.Shutdown` with a fresh bounded context (`DrainTimeout`, 5 s default). Clients receive close 1001 and should reconnect with jitter.
4. Close `Gateway` and any separate router transport, each with a fresh context.
5. Release broker clients.

Rolling deploy: one node at a time, wait until the node reports `websocket.ready=1` and the connection count recovers before the next. Reconnect target: all clients of a dead node reconnect within 60 s without taking the survivor down; keep `IPRateLimiter` burst large enough for that.
The Fiber `Server.Start` returns only after the port is bound, so a supervisor can treat a nil return as "listening". With `foundation.App`, prefer `foundation/adapter/fiber.Component` (bind in `Start`, serve in `Run`, graceful stop in `Stop`) and register the WebSocket manager shutdown as a separate component that stops *before* the listener.

## 7. Delivery matrix

| Traffic | Guarantee |
|---|---|
| Chat, lobby, presence via Router/Gateway | Best-effort. Lost on broker gap or reconnect. Gap size when a subscription is re-established is not yet measured. |
| Legacy PubSub broadcast | Best-effort, single-node/dev. |
| Results, payments, mandatory notices | Use EventBus (durable) with the `integration/eventbus` bridge. |

## 8. Troubleshooting

| Symptom | Likely cause | Check |
|---|---|---|
| Clients see 1006 | proxy idle timeout below `PongWait`, client not reading the close frame, network death | proxy timeouts; compare with close codes in CLIENT_PROTOCOL.md |
| Clients see 1013 repeatedly | slow consumer (`SlowConsumerDropThreshold` 3) or capacity | `slow_consumers`, `drops`, `MaxShardConnections` |
| 1008 after bursts | `MaxViolations` (20) consecutive rate-limit/malformed frames | client sending faster than `MessagesPerSecond` |
| 503 on upgrade | draining, node at `MaxConnections`, per-user/account/tenant quota | `/readyz`, `Retry-After` |
| `/readyz` flaps | registry slow so Gateway lease not renewed (renew budget is `LeaseTTL - RenewInterval`) | `gateway.lease_failures`, Redis latency |
| Every event gets `ErrHandlerCapacity` | blocked handlers hold permits | `handlers.quarantined`, handler timeouts |
| Stats endpoint 401 | `AdminToken` not set (closed by default) | adapter `Config` |

Measured on Linux/TLS (see [benchmarks/soak-20261126.md](benchmarks/soak-20261126.md)): about 17.5 KB heap and 4.4 goroutines per idle connection; 30 min soak with no loss, flat goroutines and no readiness flaps; Redis down 30 s and 5 min recovered in 1.6 s and 10.8 s without restarts or dropped sockets. Known limits: fan-out p99 was 277–587 ms on one shared host (target 150 ms); message loss during an outage, node kill -9, NATS outage and 24 h soak are unmeasured; `MaxViolations`/`SlowConsumerDropThreshold` are untuned. Replace the memory estimate in section 2 with these figures.

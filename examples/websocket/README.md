# WebSocket Examples

This directory is a complete runnable reference for the current WebSocket module. It demonstrates explicit manager construction, structured identities, event registration, authorization, room membership, user notifications, cluster fan-out, Redis or NATS routing, protected operational endpoints, graceful shutdown, and a protocol-aware command-line client.

This document is intentionally explicit and structured so developers, automation, and MCP-based tools can identify entry points, commands, event schemas, and expected behavior without inferring hidden setup.

## Directory Map

```text
examples/websocket/
├── main.go              server example for net/http, Gin, Echo, and Fiber
├── config.yaml          shared runnable server/client/loadtest configuration
├── .env                 equivalent flat dotenv configuration
├── README.md            this guide
├── internal/
│   └── exampleconfig/   shared strongly typed config loader
├── client/
│   └── main.go          interactive and scripted WebSocket client
└── loadtest/
    └── main.go          concurrent load and scenario generator
```

## What the Server Demonstrates

`main.go` uses the recommended production-style APIs:

- `core.NewManagerWithConfig` rather than global manager discovery.
- A manager-owned `core.EventRegistry`.
- A custom `core.Authorizer`.
- A manager-scoped `core.AtomicObserver`.
- Structured `core.Identity` authentication in every HTTP adapter.
- Explicit per-user, per-account, and per-tenant quotas.
- Bounded queues, rate limits, event execution, and handler supervision.
- Standalone, Redis Pub/Sub, or NATS broker selection.
- Correct pub/sub ownership during shutdown.
- Protected `/admin/stats` and `/admin/shard` endpoints.
- HTTP shutdown before WebSocket manager shutdown.

The example does not call `core.GetGlobalManager`, `core.RegisterHandler`, or `pubsub.SetGlobalManager`.

## Requirements

- Go version supported by this repository.
- Optional Redis when using `-broker=redis`.
- Optional NATS when using `-broker=nats`.

The checked-in `examples/websocket/config.yaml` contains local demonstration tokens and is immediately runnable. Do not reuse those credentials outside local testing.

## Configuration Loading

The server, client, and load generator all use the repository's `config` module and the same strongly typed schema. The default file is:

```text
examples/websocket/config.yaml
```

Load the dotenv example instead:

```bash
go run ./examples/websocket -config=examples/websocket/.env
go run ./examples/websocket/client -config=examples/websocket/.env
go run ./examples/websocket/loadtest -config=examples/websocket/.env
```

Configuration precedence, from lowest to highest:

1. Built-in safe defaults.
2. YAML or `.env` file selected by `-config`.
3. `WS_EXAMPLE_*` operating-system environment variables.
4. Legacy token environment variables `WS_DEMO_TOKEN` and `WS_ADMIN_TOKEN`.
5. Explicit command-line flags.

Select a file for every binary without repeating `-config`:

```bash
export WS_EXAMPLE_CONFIG=examples/websocket/.env
```

When running from `examples/websocket`, the loader also accepts a local `config.yaml` automatically.

Environment override examples:

PowerShell:

```powershell
$env:WS_EXAMPLE_SERVER_ADDR=":18080"
$env:WS_EXAMPLE_CLIENT_URL="ws://127.0.0.1:18080/ws"
$env:WS_EXAMPLE_LOADTEST_TARGET="ws://127.0.0.1:18080/ws"
$env:WS_EXAMPLE_CLIENT_TOKEN="replacement-token"
```

Bash:

```bash
export WS_EXAMPLE_SERVER_ADDR=:18080
export WS_EXAMPLE_CLIENT_URL=ws://127.0.0.1:18080/ws
export WS_EXAMPLE_LOADTEST_TARGET=ws://127.0.0.1:18080/ws
export WS_EXAMPLE_CLIENT_TOKEN=replacement-token
```

## Quick Start

The default YAML already selects standalone net/http, scripted client mode, matching tokens, and matching URLs. Start the server:

```bash
go run ./examples/websocket
```

Run the scripted client in another terminal:

```bash
go run ./examples/websocket/client
```

The scripted client performs:

1. HTTP health and stats probes.
2. WebSocket authentication with a structured identity.
3. `session.info`.
4. `system.ping`.
5. `room.join`.
6. `chat.send`.
7. `user.notify`.
8. `cluster.broadcast` when `-account=admin`.
9. `room.leave`.
10. A normal WebSocket close.

## Server Flags

| Flag | Default | Purpose |
|---|---|---|
| `-config` | `examples/websocket/config.yaml` | YAML or `.env` configuration file |
| `-stack` | Config `server_stack` | HTTP adapter: `nethttp`, `fiber`, `gin`, or `echo` |
| `-broker` | Config `server_broker` | Message router: `standalone`, `redis`, or `nats` |
| `-addr` | Config `server_addr` | HTTP listen address |
| `-redis` | Config `server_redis` | Redis address for `-broker=redis` |
| `-nats` | Config `server_nats` | NATS URL for `-broker=nats` |
| `-tenant` | Config `server_tenant` | Tenant used when a client omits `tenant_id` |

Examples:

```bash
go run ./examples/websocket -stack=gin -broker=standalone -addr=:8080
go run ./examples/websocket -stack=echo -broker=redis -redis=127.0.0.1:6379 -addr=:8080
go run ./examples/websocket -stack=fiber -broker=nats -nats=nats://127.0.0.1:4222 -addr=:8080
```

All four stacks expose the same routes and WebSocket protocol.

## HTTP Routes

| Route | Authentication | Purpose |
|---|---|---|
| `GET /ws` | Configured client bearer token | WebSocket upgrade |
| `GET /health` | Public in this local example | Manager readiness |
| `GET /admin/stats` | Configured admin bearer token | Manager, executor, observer, limiter, and broker statistics |
| `GET /admin/shard?shard=<id>` | Configured admin bearer token | One shard's connection count |

Production applications should normally protect health topology, stats, and shard metadata on an internal listener or through administrator authorization.

Probe manually:

```bash
curl http://127.0.0.1:8080/health

curl \
  -H "Authorization: Bearer local-admin-token" \
  http://127.0.0.1:8080/admin/stats
```

## WebSocket Authentication

The client sends identity fields as query parameters and the token as an HTTP header:

```text
ws://127.0.0.1:8080/ws
  ?user_id=alice
  &tenant_id=demo
  &account_id=member
  &session_id=alice-laptop

Authorization: Bearer <configured-client-token>
X-Request-ID: client-alice-laptop
```

The server creates:

```go
core.Identity{
    UserID:    "alice",
    TenantID:  "demo",
    AccountID: "member",
    SessionID: "alice-laptop",
    ExpiresAt: time.Now().Add(8 * time.Hour),
}
```

The query-based identity is acceptable only because this is a local demonstration and the bearer token is shared. A production service should derive identity from validated JWT claims, a trusted session, mTLS metadata, or authenticated middleware.

## Authorization Rules

The example `demoAuthorizer` applies two policies.

Room policy:

```text
identity tenant = demo
allowed room     = demo:lobby
denied room      = another-tenant:lobby
```

The room ID must begin with:

```text
<TenantID>:
```

Event policy:

- Every event requires non-empty `UserID` and `TenantID`.
- `cluster.broadcast` additionally requires `AccountID == "admin"`.

The authorizer runs before the business handler. Unauthorized events are dropped and counted by the observer. Room authorization errors are returned as an `error` event by the room handler.

## Event Envelope

Every client message is one WebSocket text frame containing:

```json
{
  "type": "event.name",
  "data": {}
}
```

Server messages use:

```json
{
  "type": "event.name",
  "data": {},
  "request_id": "client-session-id"
}
```

## Supported Events

### `session.info`

Purpose: inspect the authenticated identity, assigned shard, request ID, and server time.

Request:

```json
{
  "type": "session.info",
  "data": {}
}
```

Response type:

```text
session.info.result
```

Response data contains:

```json
{
  "identity": {
    "UserID": "alice",
    "TenantID": "demo",
    "AccountID": "member",
    "SessionID": "alice-laptop",
    "ExpiresAt": "..."
  },
  "shard_id": "shard-...",
  "request_id": "client-alice-laptop",
  "server_time": "..."
}
```

### `system.ping`

Purpose: application-level request/response demonstration. This is separate from protocol-level WebSocket ping/pong frames managed by the connection pumps.

Request:

```json
{
  "type": "system.ping",
  "data": {}
}
```

Response type:

```text
system.pong
```

### `room.join`

Purpose: authorize and add the current physical connection to a room.

Request:

```json
{
  "type": "room.join",
  "data": {
    "room_id": "demo:lobby"
  }
}
```

Response type:

```text
room.joined
```

Joining is idempotent. The response includes the physical connection count currently in the room on this node.

### `room.leave`

Request:

```json
{
  "type": "room.leave",
  "data": {
    "room_id": "demo:lobby"
  }
}
```

Response type:

```text
room.left
```

Leaving is idempotent.

### `chat.send`

Purpose: fan out a chat-style message to local room members and peer nodes.

Request:

```json
{
  "type": "chat.send",
  "data": {
    "room_id": "demo:lobby",
    "text": "hello room"
  }
}
```

Received event type:

```text
chat.message
```

The handler calls both:

```go
shard.SendToRoom(roomID, payload)
shard.BroadcastRoomMessage(roomID, payload)
```

`SendToRoom` reaches local room members. `BroadcastRoomMessage` publishes to peer cluster nodes. Publishing alone does not perform local fan-out.

This example does not persist chat messages. A production chat service should store the message first, then publish the live update.

### `user.notify`

Purpose: send a notification to all active sessions for one user locally and across peer nodes.

Request:

```json
{
  "type": "user.notify",
  "data": {
    "user_id": "bob",
    "text": "you have a new notification"
  }
}
```

Received event type:

```text
user.notification
```

The example invokes local and cross-node paths:

```go
shard.SendToUser(userID, payload)
shard.BroadcastUserNotification(userID, payload)
```

### `cluster.broadcast`

Purpose: broadcast to every connection in the sender's shard locally and on peer nodes.

Authorization requirement:

```text
AccountID == "admin"
```

Request:

```json
{
  "type": "cluster.broadcast",
  "data": {
    "text": "maintenance starts soon"
  }
}
```

Received event type:

```text
cluster.broadcast
```

The example calls `Shard.BroadcastGlobal`, which performs local broadcast and broker publication.

### `error`

Business handlers return validation or room-operation errors as:

```json
{
  "type": "error",
  "data": {
    "source": "room.join",
    "message": "websocket operation unauthorized"
  },
  "request_id": "client-alice-laptop"
}
```

Authorization failures rejected before handler execution are counted as dropped events and do not automatically generate this response.

## Client Flags

| Flag | Default | Purpose |
|---|---|---|
| `-config` | `examples/websocket/config.yaml` | YAML or `.env` configuration file |
| `-url` | Config `client_url` | WebSocket endpoint |
| `-user` | Config `client_user` | Identity `UserID` |
| `-tenant` | Config `client_tenant` | Identity `TenantID` |
| `-account` | Config `client_account` | Identity `AccountID`; use `admin` for cluster broadcast |
| `-session` | Config or generated | Identity `SessionID` |
| `-token` | Config `client_token` | Client bearer token |
| `-admin-token` | Config `admin_token` or client token | Admin probe token |
| `-mode` | Config `client_mode` | `interactive`, `script`, or `single` |
| `-room` | Config `client_room` | Default room |
| `-message` | Config `client_message` | Message for script or single mode |
| `-target-user` | Config `client_target_user` | Recipient for scripted notification |
| `-probe` | Config `client_probe` | Probe health and stats before connecting |

## Interactive Client Commands

Start:

```bash
go run ./examples/websocket/client \
  -mode=interactive \
  -user=alice \
  -tenant=demo \
  -account=admin
```

Commands:

```text
/info
/ping
/join demo:lobby
/leave demo:lobby
/chat demo:lobby hello everyone
/notify bob hello bob
/broadcast maintenance starts soon
/raw {"type":"system.ping","data":{}}
/help
/quit
```

A line without a slash is sent as `chat.send` to the default `-room`.

## Scripted and Single Modes

Scripted protocol walkthrough:

```bash
go run ./examples/websocket/client \
  -mode=script \
  -user=alice \
  -tenant=demo \
  -account=admin \
  -target-user=bob
```

Send one room message and wait briefly for responses:

```bash
go run ./examples/websocket/client \
  -mode=single \
  -user=alice \
  -room=demo:lobby \
  -message="one message"
```

Scripted mode is suitable for smoke tests and MCP-driven verification because it does not require stdin interaction.

## Multi-Client Room Example

Terminal 1, start server:

```bash
go run ./examples/websocket -stack=nethttp -broker=standalone
```

Terminal 2, Bob joins the room:

```bash
go run ./examples/websocket/client -mode=interactive -user=bob -room=demo:lobby
```

Then enter:

```text
/join demo:lobby
```

Terminal 3, Alice joins and sends:

```bash
go run ./examples/websocket/client -mode=interactive -user=alice -room=demo:lobby
```

Then enter:

```text
/join demo:lobby
/chat demo:lobby hello bob
```

Both clients should receive `chat.message`.

## Multi-Device User Example

Run two clients with the same user but different sessions:

```bash
go run ./examples/websocket/client -mode=interactive -user=alice -session=alice-laptop
go run ./examples/websocket/client -mode=interactive -user=alice -session=alice-phone
```

From another client:

```text
/notify alice notification for every alice session
```

Both Alice sessions should receive `user.notification`, subject to the configured per-user connection limit.

## Redis Cluster Example

Start Redis. The repository includes queue infrastructure under `docker/queue`; any reachable Redis instance is sufficient for this example.

Start node A:

```bash
go run ./examples/websocket \
  -stack=nethttp \
  -broker=redis \
  -redis=127.0.0.1:6379 \
  -addr=:8080
```

Start node B:

```bash
go run ./examples/websocket \
  -stack=gin \
  -broker=redis \
  -redis=127.0.0.1:6379 \
  -addr=:8081
```

Connect Bob to node B:

```bash
go run ./examples/websocket/client \
  -url=ws://127.0.0.1:8081/ws \
  -mode=interactive \
  -user=bob
```

Join:

```text
/join demo:lobby
```

Connect Alice to node A and send:

```bash
go run ./examples/websocket/client \
  -url=ws://127.0.0.1:8080/ws \
  -mode=interactive \
  -user=alice
```

```text
/join demo:lobby
/chat demo:lobby cross-node hello
```

Bob should receive the room message through Redis Pub/Sub.

## NATS Cluster Example

Run the same two-node scenario with:

```bash
-broker=nats -nats=nats://127.0.0.1:4222
```

Core NATS remains best-effort. The example does not use JetStream persistence.

## Framework Matrix

| Stack | Server command |
|---|---|
| net/http | `go run ./examples/websocket -stack=nethttp` |
| Gin | `go run ./examples/websocket -stack=gin` |
| Echo | `go run ./examples/websocket -stack=echo` |
| Fiber | `go run ./examples/websocket -stack=fiber` |

The client command does not change when switching stacks.

## Manager Configuration Used by the Example

| Setting | Value |
|---|---:|
| `MaxConnections` | 20,000 |
| `MaxShards` | 64 |
| `MaxShardConnections` | 1,000 |
| `SendQueueSize` | 128 |
| `InboundQueueSize` | 1,024 |
| `MaxMessageBytes` | 16 KiB |
| `MessagesPerSecond` | 20 |
| `MessageBurst` | 50 |
| `BytesPerSecond` | 32 KiB |
| `ByteBurst` | 64 KiB |
| `MaxPendingHandlers` | 16 per connection |
| `MaxHandlerGoroutines` | 64 per manager |
| `MaxRoomsPerConnection` | 50 |
| `SlowConsumerDropThreshold` | 2 consecutive full queues |
| `MaxConnectionsPerUser` | 5 per node |
| `MaxConnectionsPerAccount` | 100 per node |
| `MaxConnectionsPerTenant` | 5,000 per node |
| `WriteWait` | 5 seconds |
| `PongWait` | 60 seconds |
| `PingPeriod` | 50 seconds |

These values are example limits, not production capacity claims.

## Graceful Shutdown Sequence

On `SIGINT` or `SIGTERM`, the server:

1. Stops HTTP admission.
2. Calls `manager.Shutdown` with a 15-second deadline.
3. Closes active connection pumps and shards.
4. Drains the owned event executor.
5. Unregisters manager-specific pub/sub handlers.
6. Shuts down the owned Redis/NATS pub/sub manager.
7. Closes the underlying Redis or NATS client.
8. Closes the logger.

The server also performs cleanup when the HTTP listener exits unexpectedly.

## Failure and Error Examples

Wrong client token:

```text
HTTP 401 unauthorized
```

Join another tenant's room:

```text
/join private:lobby
```

Expected response:

```text
error source=room.join
```

Broadcast as a non-admin account:

```text
/broadcast hello
```

Expected behavior:

- The authorizer drops the event before handler execution.
- Observer `EventDropped` increases.
- No broadcast is sent.
- No automatic error envelope is generated for pre-handler authorization rejection.

Broker unavailable with `-broker=redis` or `-broker=nats`:

- Application startup exits with an error.
- The example does not silently downgrade to standalone mode.

## Load Generator

The separate load generator supports:

```text
message
idle
churn
fanout
reconnect
rolling-drain
```

Example:

```bash
go run ./examples/websocket/loadtest
```

Override only the desired load values:

```bash
go run ./examples/websocket/loadtest -connections=1000 -messages=20 -scenario=message
```

The load generator reads `loadtest_*`, client credentials, tenant, and account from the shared file. It appends a unique structured identity to every connection. Its default `system.ping` event always receives a response, making the latency metrics meaningful.

The load generator is intended for repeatable stress scenarios. The protocol-aware client is intended for functional examples and manual inspection.

## Full Local Test

Use the checked-in YAML without exporting any variables.

Terminal 1:

```bash
go run ./examples/websocket
```

Terminal 2, full functional protocol scenario:

```bash
go run ./examples/websocket/client
```

Terminal 2, load scenario using the same URL and token:

```bash
go run ./examples/websocket/loadtest
```

To test `.env` loading, add the same `-config` value to all three commands:

```bash
go run ./examples/websocket -config=examples/websocket/.env
go run ./examples/websocket/client -config=examples/websocket/.env
go run ./examples/websocket/loadtest -config=examples/websocket/.env
```

## Mapping to Production

Replace these demonstration choices:

| Example choice | Production replacement |
|---|---|
| Shared static bearer token | JWT, secure session, mTLS, or trusted identity middleware |
| Query identity fields | Claims from authenticated context |
| In-memory chat event | Persist message before fan-out |
| Public health route | Internal listener or infrastructure authorization |
| Static authorizer | Domain permission service or cached policy |
| Node-local quotas only | Distributed reservation if strict cluster quota is required |
| Redis Pub/Sub/Core NATS | Durable backend for messages that cannot be lost |
| Hard-coded limits | Values established by Linux/TLS load and soak testing |

## Build and Test

```bash
go build ./examples/websocket
go build ./examples/websocket/client
go build ./examples/websocket/loadtest
go test ./websocket/...
```

Run static checks from the repository root:

```bash
go vet ./...
git diff --check
```

## Related Documentation

- `websocket/README.md`: complete module architecture, configuration, security, delivery, and tuning guide.
- `WEBSOCKET_P0_REPORT.md`: dependency ownership, supervision, shard synchronization, and room cleanup.
- `WEBSOCKET_P1_REPORT.md`: payload ownership, quotas, observers, broker recovery, and durable contract.
- `WEBSOCKET_P2_REPORT.md`: latency, tracing, logging, profiles, and load scenarios.
- `WEBSOCKET_DEFINITION_OF_DONE_REPORT.md`: local completion versus external production evidence.

# Gateway / worker / router example

## WebSocket module capabilities

| Capability | Behavior |
|---|---|
| WebSocket connections | Adapters support net/http, Gin, Echo, and Fiber. This cluster example uses net/http. |
| Connection management | The manager assigns connections to shards and manages read/write pumps, ping/pong, timeouts, and shutdown. |
| Identity and resource limits | Supports authentication, authorization, quotas, rate limits, bounded queues, and configurable slow-consumer disconnection. |
| Local messaging | Sends to a connection, user, or room, or broadcasts within a shard. A user can have multiple sessions/devices. |
| Cluster messaging | The router delivers to a user, a specific session, or a room across shards/nodes. The new routing APIs isolate targets by tenant. |
| Session tracking | Redis stores locations, lease TTLs, and generations. Reconnecting with the same session ID updates the destination gateway. |
| Client event dispatch | `Dispatch: Local` runs callbacks through a bounded executor. `Dispatch: Business` publishes commands through the EventBus bridge. |
| Operations | Provides readiness, counters, queue-wait metrics, benchmarks, and enqueue-latency sampling tools. |

The module supports standalone operation, legacy Redis/NATS pub/sub, and opt-in
targeted routing through `ManagerConfig.Distributed`. In distributed mode, the
manager substitutes a noop legacy pub/sub backend to avoid duplicate publication
paths. Applications use the Router for cross-node messaging. Local shard APIs
retain their shard-scoped behavior.

### How WebSocket and EventBus work together

WebSocket maintains connections and places outgoing data into connection queues.
EventBus transports business commands/events, distributes work among worker/router
instances, and handles retry/DLQ behavior. The Redis registry identifies the nodes
hosting a session or room; targeted transport delivers messages to those nodes.

| Component / example role | Responsibility |
|---|---|
| `gateway` | Authenticates clients, manages connections/shards/rooms, registers sessions, runs local handlers or publishes business commands, enqueues payloads, and writes socket frames. |
| `worker` | Instances compete for Task `ws.command`, execute business logic, and publish Event `ws.notification`. |
| `router` | Instances share subscriber group `realtime-router`, divide Event `ws.notification` processing, resolve locations, and deliver to destination gateways. |
| `publish` | An external producer that publishes user or room notifications through EventBus. |
| `load` | Calls the Router directly to measure the round trip to gateway enqueue. It bypasses business Tasks/Events. |

| Configuration | Business Task/Event | Targeted delivery + result | Session/room registry |
|---|---|---|---|
| `-broker redis` | EventBus Redis Streams driver | Redis Pub/Sub | Redis |
| `-broker nats` | EventBus NATS JetStream driver | NATS Core | Redis |

Each gateway uses two fixed delivery/result subscriptions per node incarnation,
independent of connection count. Gateways, workers, and routers connect to the
broker. Gateways do not establish WebSocket connections to each other.

## Overall flow

```mermaid
flowchart TD
    CA["Client A"] -->|"WebSocket frame"| GA["Gateway A: authenticate, validate, authorize"]
    CB["Client B"] -->|"WebSocket connection"| GB["Gateway B: connection manager"]
    GA --> D{"Dispatch policy"}
    D -->|"Local: room.join / chat.send"| L["Local handler through bounded executor"]
    D -->|"Business: business"| B["EventBus Bridge: connection identity"]
    B --> T["EventBus Task: ws.command"]
    T --> W["Business worker group"]
    W -->|"PublishNotification"| E["EventBus Event: ws.notification"]
    P["External producer: role publish"] --> E
    E --> R["Subscriber group: realtime-router"]
    R -->|"Resolve user/session or room nodes"| REG[("Redis Session/Room Registry")]
    REG -->|"Destination nodes"| R
    R --> X["Targeted transport: Redis Pub/Sub or NATS Core"]
    L -->|"chat.send: direct Router call"| RLOCAL["Router in gateway process"]
    RLOCAL --> REG
    REG --> RLOCAL
    RLOCAL --> X
    X -->|"DeliveryCommand for node A"| IA["Gateway A: validate target, lease, generation, dedupe"]
    X -->|"DeliveryCommand for node B"| IB["Gateway B: validate target, lease, generation, dedupe"]
    IA --> QA["Per-connection outbound queues at A"]
    IB --> QB["Per-connection outbound queues at B"]
    QA -->|"write pump / text frame"| CA
    QB -->|"write pump / text frame"| CB
```

The arrows to A/B illustrate possible destinations. User/session delivery targets
only nodes hosting the resolved sessions; room delivery targets nodes with room
membership. Notifications are not broadcast to every node. `room.join` updates
local/Redis membership and returns `room.joined` directly through the connection
queue, without EventBus.

## Business flow: client → worker → notification → client

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Source gateway
    participant EB as EventBus
    participant W as Business worker
    participant R as Realtime router group
    participant REG as Redis Registry
    participant X as Targeted transport
    participant DEST as Destination gateway

    C->>G: Frame type=business, data=payload
    G->>G: Validate, authorize, rate/admission limits
    G->>EB: Enqueue Task ws.command through bridge
    Note over G,EB: Tenant/User/Account/Session come from authenticated identity
    EB->>W: One worker receives the task
    W->>W: Execute business logic
    W->>EB: Publish Event ws.notification
    Note over W,EB: WebSocket payload has type=business.completed
    W-->>EB: Task handler succeeds and EventBus settles task
    EB->>R: One instance in group realtime-router receives the event
    R->>REG: Resolve tenant + user and current sessions
    REG-->>R: Locations with node, shard, session, generation
    R->>X: DeliveryCommand, with sessions grouped by node
    X->>DEST: Deliver to the gateway hosting the sessions
    DEST->>DEST: Validate destination, expiry, generation, dedupe
    DEST->>DEST: Enqueue payload in connection queues
    DEST-->>X: DeliveryResult after enqueue
    X-->>R: Enqueued or partial/error results
    alt Enqueued or user offline
        R-->>EB: Event handler succeeds and EventBus settles event
    else Queue full, unavailable, or unknown
        R-->>EB: Handler returns an error
        EB->>EB: Retry according to policy then move to DLQ after attempts are exhausted
    end
    DEST-->>C: Write pump sends frame when processing the queue
```

The destination gateway may be the source gateway or a different node after
reconnect. Once EventBus accepts a task, the worker continues processing it even
if the client disconnects. This example echoes the business payload. Applications
replace that handler with business logic and manage DB transactions/outboxes
where required.

The EventBus event in this example is **`ws.notification`**. `business.completed`
is the `type` field in the frame sent to the client. Custom business events such
as `order.completed` can be integrated through `bridge.Mapping`; this example
does not register such a mapping.

## Chat room flow: local handler → room across shards/nodes

```mermaid
sequenceDiagram
    participant A as Client A
    participant GA as Gateway A
    participant REG as Redis Registry
    participant X as Targeted transport
    participant GB as Gateway B
    participant B as Client B

    A->>GA: room.join, room_id=demo:room
    GA->>REG: Publish session membership in the room
    GA-->>A: room.joined after successful join
    B->>GB: room.join, room_id=demo:room
    GB->>REG: Publish session membership in the room
    GB-->>B: room.joined after successful join
    A->>GA: chat.send, room_id=demo:room, text=hello
    GA->>GA: Local handler creates payload type=chat.message
    GA->>REG: Router.SendToRoom resolves nodes hosting the room
    REG-->>GA: Node A and node B
    GA->>X: DeliveryCommand for each node
    X->>GA: Room delivery at A
    X->>GB: Room delivery at B
    GA->>GA: Fan out to all local shards with room members
    GB->>GB: Fan out to all local shards with room members
    GA-->>X: Enqueue results and recipient counts at A
    GB-->>X: Enqueue results and recipient counts at B
    X-->>GA: Router aggregates results
    GA-->>A: Write pump sends chat.message
    GB-->>B: Write pump sends chat.message
```

In this example, `chat.send` uses targeted transport directly. It does not publish
Task `ws.command` or Event `ws.notification`. A local handler can still call Redis
and the broker; "Local" describes where the callback executes. To route chat
through a business worker for persistence or moderation, applications must register
the corresponding dispatch policy and worker.

## Reconnect flow: a session moves to another gateway

```mermaid
sequenceDiagram
    participant C as Client with the same session ID
    participant A as Gateway A
    participant REG as Redis Registry
    participant B as Gateway B
    participant R as Router
    participant X as Targeted transport

    C->>A: Connect with tenant + user + session
    A->>REG: Register location A, generation g1
    C--xA: Connection lost
    C->>B: Reconnect with the same tenant + user + session
    B->>REG: Register location B, generation g2
    REG-->>B: New location replaces the previous location
    A->>REG: Old cleanup with generation g1
    REG-->>A: Preserve registration g2
    R->>REG: Resolve when a new notification arrives
    REG-->>R: Gateway B, generation g2
    R->>X: DeliveryCommand to B
    X->>B: Validate generation g2 and enqueue
    B-->>X: DeliveryResult
    X-->>R: Enqueue result
    B-->>C: Write pump sends frame
```

If the location changes between resolution and delivery, a `stale`/`not_found`
result triggers up to two re-resolves. Fencing and leases do not synchronously
revoke every old connection. A superseded session on the old node is closed when
renewal detects the new generation. Without a supplied session ID, the gateway
creates a new ID per connection and does not assume two connections represent
the same device.

### Delivery guarantees

- `enqueued` confirms acceptance into a gateway queue. Socket writes or client
  receipt can still fail afterward. The relative order of the enqueue ACK and
  socket write depends on scheduling because the write pump runs independently.
- A missing delivery result is `unknown`. Retries reuse the same message ID for
  deduplication. Dedupe has TTL/capacity limits and does not guarantee exactly-once
  delivery.
- Offline is a terminal bridge outcome, so the EventBus handler succeeds. There
  is currently no offline inbox, reconnect replay, or client ACK.
- The Redis registry is required even with NATS. An expired lease prevents new
  gateway connection admission/delivery until renewal/reconciliation succeeds.

## Running the example

Requires Redis for the session registry. Choose either Redis Streams + Redis
Pub/Sub or NATS JetStream + NATS Core. Start NATS with JetStream enabled (`-js`).
Use the same broker/prefix configuration for every role.

Run in separate terminals (replace `-broker redis` with `-broker nats` for NATS):

```bash
export WS_EXAMPLE_CLIENT_TOKEN=local-demo-token

go run ./examples/websocket/cluster -broker redis -role gateway -addr 127.0.0.1:8080
go run ./examples/websocket/cluster -broker redis -role gateway -addr 127.0.0.1:8081
go run ./examples/websocket/cluster -broker redis -role worker
go run ./examples/websocket/cluster -broker redis -role router
```

Connect two interactive clients to different gateways:

```bash
go run ./examples/websocket/client -url ws://127.0.0.1:8080/ws -tenant demo -user alice -session alice-device -token local-demo-token -probe=false
go run ./examples/websocket/client -url ws://127.0.0.1:8081/ws -tenant demo -user bob -session bob-device -token local-demo-token -probe=false
```

On both clients, enter `/join demo:room`. Enter `/chat demo:room hello` on one;
both receive `chat.message` despite being on different shards/nodes.

Publish through EventBus to a user or room:

```bash
go run ./examples/websocket/cluster -broker redis -role publish -tenant demo -user alice -text hello
go run ./examples/websocket/cluster -broker redis -role publish -tenant demo -room demo:room -text hello
```

Send this client frame using a WebSocket client to exercise the business worker:

```json
{"type":"business","data":{"action":"example"}}
```

The worker publishes EventBus event `ws.notification`, containing a WebSocket
payload with type `business.completed`; the router resolves the user's current
sessions and targets their gateway. Reconnect `alice-device` to port
8081 and publish again to exercise location replacement. The notification is
not stored if Alice is offline.

Measure enqueue round-trip p95/p99 with connected clients:

```bash
go run ./examples/websocket/cluster -broker redis -role load -tenant demo -user alice -count 10000
go run ./examples/websocket/cluster -broker nats -role load -tenant demo -room demo:room -count 10000
```

`load` rejects offline or partially failed samples rather than reporting them as
successful throughput. It measures routing through the broker to gateway enqueue,
including registry lookup; it does not measure client receipt. Use the existing
`examples/websocket/loadtest` tooling for connection churn and socket workloads.

This is a local demonstration: one bearer token permits choosing identity query
parameters. Production authentication must derive tenant/user/session from a
verified token. Apply room membership/business authorization in the application.
The example supports `room.join`, `chat.send`, and `business`; other commands of
the original interactive client are not implemented here.

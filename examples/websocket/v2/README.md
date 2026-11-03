# Complete WebSocket v2 example

A runnable reference for the current `go-common/v2/websocket` APIs. Each process
hosts a net/http WebSocket gateway, a business worker and a notification router.
Run two processes with the same namespace to exercise cross-node routing. The
[separate-role cluster example](../cluster/README.md) shows how to deploy those
roles independently, including Kafka EventBus.

This example uses Redis session leases with either Redis Pub/Sub or NATS Core
delivery. Business commands use Redis EventBus by default; memory EventBus is an
explicit process-local development option. It never changes process-global
manager, handler, authorization, pub/sub or EventBus driver defaults.

## Files and responsibilities

| File | Demonstrates |
|---|---|
| [main.go](main.go) | CLI settings, signal handling, HTTP admission and shutdown |
| [app.go](app.go) | Explicit Redis/NATS, registry, Gateway, Router, EventBus, bridge and Manager ownership |
| [auth.go](auth.go) | Environment credentials mapped to trusted tenant/user/device identities; room and event policy |
| [handlers.go](handlers.go) | Validation, local/Business dispatch, user/session/room sends, readiness and admin endpoints |
| [intake.go](intake.go) | Stop publication from existing sockets before draining EventBus |
| [client/main.go](client/main.go) | Scripted protocol walkthrough, room listener and admin notification producer |
| [compose.yaml](compose.yaml) | Local-only Redis and optional NATS dependencies |
| [app_test.go](app_test.go) | Two-gateway routing, tenant isolation, dedupe, real NATS callbacks and lifecycle tests |

## Start local dependencies

Run all commands from the repository root with the Go version required by go.mod.

```bash
docker compose -f examples/websocket/v2/compose.yaml up -d
docker compose -f examples/websocket/v2/compose.yaml exec redis redis-cli ping
```

Wait for `PONG` before starting the server. Redis is available on **127.0.0.1:16379**.
For NATS delivery, start the optional profile as well:

```bash
docker compose -f examples/websocket/v2/compose.yaml --profile nats up -d
```

NATS is available on **127.0.0.1:14222**. JetStream is not needed: business EventBus
still uses Redis. The ports deliberately differ from other examples/integration
tests. Brokers are published only on loopback and have no production auth/TLS
configuration. Use deployment-managed credentials, TLS and persistence outside
local development. Removing the Compose containers removes their local data.

## Credentials and trusted identities

Generate credentials once and provide the same environment to the server and
client terminals. Do not regenerate values independently for each gateway. For
example, in a Bash shell with OpenSSL:

```bash
export WS_V2_ALICE_TOKEN="$(openssl rand -hex 32)"
export WS_V2_ALICE_PHONE_TOKEN="$(openssl rand -hex 32)"
export WS_V2_BOB_TOKEN="$(openssl rand -hex 32)"
export WS_V2_OTHER_TOKEN="$(openssl rand -hex 32)"
export WS_V2_ADMIN_TOKEN="$(openssl rand -hex 32)"
export WS_V2_TOKEN="$WS_V2_ALICE_TOKEN"
```

PowerShell 7 equivalent (assignments do not print the generated credentials):

```powershell
$env:WS_V2_ALICE_TOKEN = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
$env:WS_V2_ALICE_PHONE_TOKEN = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
$env:WS_V2_BOB_TOKEN = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
$env:WS_V2_OTHER_TOKEN = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
$env:WS_V2_ADMIN_TOKEN = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
$env:WS_V2_TOKEN = $env:WS_V2_ALICE_TOKEN
```

Keep real credentials in your environment/secret manager, not repository files or
command arguments. At least one client credential and a separate admin credential
are required. All configured credentials must be unique and at least 32 bytes.

| Server credential environment | Trusted tenant | User | Stable session |
|---|---|---|---|
| WS_V2_ALICE_TOKEN | demo | alice | desktop |
| WS_V2_ALICE_PHONE_TOKEN | demo | alice | phone |
| WS_V2_BOB_TOKEN | demo | bob | desktop |
| WS_V2_OTHER_TOKEN | other | alice | desktop |
| WS_V2_ADMIN_TOKEN | Administrative HTTP access only | — | — |

These identities are defined by the server, not URL parameters or client JSON.
AccountID is a server-derived tenant member account. Demo identities expire one
hour after process startup; expired credentials are rejected and existing sockets
are closed by Identity.ExpiresAt. Replace this static lookup with verified
application sessions/JWTs, revocation and membership policies for production.

The policy permits only the authenticated tenant's `<tenant>:lobby` room. A normal
socket can notify its own user or current session. Sending to arbitrary targets
requires the separate admin credential. The same credential on another gateway
represents the same stable device session and causes location takeover; use the
phone credential for a second independent Alice device.

## Run one gateway and the protocol walkthrough

With the credential environment available in both terminals:

```bash
go run ./examples/websocket/v2 -addr=127.0.0.1:8090
```

In the client terminal:

```bash
go run ./examples/websocket/v2/client -mode=script
```

The client authenticates with `WS_V2_TOKEN`. It requests session information, joins
the lobby, pings, sends a room message, notifies all of its devices, notifies its
current session, submits a business command, waits for the routed completion,
leaves the room and closes normally. Every step waits for its protocol response;
background notifications are printed as JSON. The server does not print tokens,
credential-bearing URLs or message payloads.

For a complete one-shell Bash run, build the server so its PID is the actual
binary, inherit the credential environment, and terminate it gracefully afterward:

```bash
go build -o "${TMPDIR:-/tmp}/ws-v2-demo" ./examples/websocket/v2
"${TMPDIR:-/tmp}/ws-v2-demo" -addr=127.0.0.1:8090 &
GATEWAY_PID=$!
go run ./examples/websocket/v2/client -mode=script
kill -TERM "$GATEWAY_PID"
wait "$GATEWAY_PID"
```

Use foreground terminals and Ctrl+C on Windows to exercise the signal/shutdown
path. `go run ... -h` prints server/client flags without credential values.

## Two gateways and two devices

Run a second server with the same namespace and credential environment:

```bash
go run ./examples/websocket/v2 -addr=127.0.0.1:8091
```

Keep Alice's desktop listening on gateway A:

```bash
export WS_V2_TOKEN="$WS_V2_ALICE_TOKEN"
go run ./examples/websocket/v2/client -mode=listen -url=ws://127.0.0.1:8090/ws
```

In another terminal, select Alice's phone on gateway B:

```bash
export WS_V2_TOKEN="$WS_V2_ALICE_PHONE_TOKEN"
go run ./examples/websocket/v2/client -mode=script -url=ws://127.0.0.1:8091/ws
```

The desktop receives phone-originated room messages and user notifications. A
session notification and the business completion go only to the originating
phone session. Bob may join `demo:lobby` using his own credential. The other
tenant's Alice must use `-room=other:lobby` and receives no demo-tenant messages.
For PowerShell, select credentials with `$env:WS_V2_TOKEN = $env:WS_V2_ALICE_PHONE_TOKEN`.

Switch delivery without changing the EventBus broker:

```bash
go run ./examples/websocket/v2 -addr=127.0.0.1:8090 -delivery=nats
```

All gateway processes in the namespace must use the same delivery backend and
prefix. Redis is still required for session/room leases and Redis EventBus.
Use `-eventbus=memory` only for process-local development: no restart persistence
or cluster-wide business consumer competition is provided by that driver.

## External user, session and room notifications

The notification producer uses `WS_V2_ADMIN_TOKEN`; it does not open a WebSocket.

```bash
# Every demo/alice device; repeat this command with the same ID to exercise dedupe.
go run ./examples/websocket/v2/client -mode=notify -user=alice -message-id=demo-notification-1

# A specific authenticated device.
go run ./examples/websocket/v2/client -mode=notify -user=alice -session=phone

# Every local member across all gateways hosting this room.
go run ./examples/websocket/v2/client -mode=notify -user='' -room=demo:lobby
```

The HTTP equivalent is `POST /admin/notifications` with bearer admin authorization
and this body (omit session_id to target all user devices):

```json
{"message_id":"logical-notification-42","tenant_id":"demo","user_id":"alice","session_id":"phone","text":"hello"}
```

For a room, omit user_id/session_id and set room_id. Mixing selectors is a 400.
Successful routing returns 200 with message_id and DeliveryResult. Unknown,
partial or unavailable delivery returns 503 **with the result**, not a claim that
no recipient accepted anything. Retry the same logical message ID and payload.
Offline returns 200 with Offline set; the example does not persist an offline inbox.

## HTTP and WebSocket contract

| HTTP route | Authorization | Purpose |
|---|---|---|
| GET /ws | Client bearer credential; default same-origin policy | Upgrade and attach trusted identity |
| GET /livez | Public | Process liveness |
| GET /readyz | Public | Intake state, manager readiness and EventBus Ping |
| GET /admin/stats | Separate admin bearer | Manager, gateway and router snapshots; no per-shard identifier map |
| POST /admin/notifications | Separate admin bearer | User/session/room routing with bounded JSON body |

Browser WebSocket cannot set this example's Authorization header. Use the Go
client, or design verified cookie/ticket authentication for a browser deployment;
do not move long-lived bearer credentials into URLs or disable the origin policy.

Incoming envelope: `{"type":"event.name","data":{...}}`.

| Event | Data | Result |
|---|---|---|
| session.info | `{}` | session.info with server-selected identity and shard_id |
| system.ping | `{}` | system.pong |
| room.join / room.leave | `{"room_id":"demo:lobby"}` | room.joined / room.left |
| chat.send | `{"room_id":"demo:lobby","text":"hello"}` | chat.message to room members, chat.sent with recipient counts to sender |
| notify.user | `{"text":"hello"}` | notification to the authenticated user's devices, notify.user.sent |
| notify.session | `{"text":"hello"}` | notification only to current authenticated session, notify.session.sent |
| business.echo | `{"text":"work"}` | EventBus command; worker publishes business.completed through the bridge/router |

Text is limited to 512 characters, event data to 2 KiB, inbound frames to 8 KiB
and admin bodies to 4 KiB. Payload decoding rejects unknown fields and trailing
JSON. Core counts invalid/unknown/unauthorized envelopes rather than automatically
acknowledging them; malformed input may have no response. Local handler errors
produce an error frame with a generic code. A successful handler response is not
a client-receipt guarantee for every routed recipient.

## Business work and shutdown

`app.go` opens EventBus with WithTransport, declares the bridge before Bus.Start,
registers a ws.command worker, and wires Dispatch: Business through CommandPublisher.
For Redis, notification topology uses WithEventLayoutTargeted and short bounded
polls. Every gateway hosts the same realtime-router subscriber group, so Redis
EventBus instances compete for notifications; delivery is still targeted by leases.

The echo worker has no external side effect and uses a stable command-derived
notification ID. Real business writes need application idempotency/outbox/inbox.
Redis persistence depends on its deployment; memory EventBus is process-local.
Gateway dedupe is bounded, in memory and generation-scoped. Client ACK, replay and
an offline inbox are not supplied by either WebSocket transport.

On Ctrl+C/SIGTERM:

1. Mark readiness false; reject upgrades, admin publication and new socket events.
2. Stop CommandPublisher admission and wait for in-flight publications.
3. Drain accepted command and notification deliveries, then drain EventBus while routing/sockets remain available.
4. Drain Manager's accepted outbound queues; retain timeout/force-close errors and counters.
5. Close Gateway/transport, retry a timed-out gateway close once with a fresh budget, then release Redis/NATS clients. Cleanup failures remain in the process result.

Each phase receives a fresh bounded context; the phase budget is drain-timeout
plus one second. The HTTP phase has a separate five-second budget. Total shutdown
can span multiple phase budgets. DrainTopology pauses local intake and waits for
already accepted deliveries, not the whole distributed broker backlog. Remaining
durable work needs another consumer/restart and application resynchronization.
Core's drain guarantee covers accepted outbound queue entries, not all business
publications or client receipt. Cleanup errors affect the process result.

## Settings

| Flag | Default |
|---|---|
| -addr | 127.0.0.1:8090 |
| -namespace | ws-v2-demo |
| -delivery | redis; alternative nats |
| -eventbus | redis; alternative memory |
| -registry-concurrency | 32 |
| -dedupe-capacity | 100000, divided across cache stripes |
| -lease-ttl / -renew-interval | 30s / 10s |
| -drain-timeout | 5s outbound budget |
| -room-node-index | false; explicit rollout opt-in |

Credential-bearing broker URLs are environment-only:
WS_V2_REDIS_URL defaults to `redis://127.0.0.1:16379/0`;
WS_V2_NATS_URL defaults to `nats://127.0.0.1:14222`.
`rediss://` keeps certificate verification enabled. The Redis client enables
ContextTimeoutEnabled and bounded dial/read/write timeouts.

The example configures 2,000 node-local connections, 4 per user, 100 per account,
500 per tenant, 8 rooms per connection, a 64-message send queue, four executor
partitions and 16 supervised handlers. These are demo limits, not capacity claims.
Core quotas use raw identity dimension values: the demo's two tenant-local
`alice` IDs share the per-user quota on a node, while routing remains tenant-scoped.
Choose globally unique IDs or an intentional shared-quota policy for production.

Indexed room reads may be enabled for a new isolated namespace or after all
writers are upgraded and old memberships expire/refresh. Keep compatibility reads
during mixed-version rollout. See the [module routing guide](../../../websocket/routing/README.md#room-lookup-and-node-index-rollout).

## Verification and cleanup

```bash
go test ./examples/websocket/v2/...
go test -race -count=10 ./examples/websocket/v2/...
go vet ./examples/websocket/v2/...
```

Tests exercise Redis delivery with memory and Redis EventBus, plus real embedded
NATS Core delivery with Redis EventBus. Redis is miniredis in tests; no external
broker/credentials are required. The matrix covers query-identity spoofing,
tenant isolation, user/session/room routing, duplicate notification IDs, room
leave, the business round trip, admin separation, body/target limits and shutdown.
Validate real Redis, TLS, failover and workloads separately before deployment.

After stopping the example processes:

```bash
docker compose -f examples/websocket/v2/compose.yaml --profile nats down
```

See the [WebSocket v2 guide](../../../websocket/README.md) for API defaults,
guarantees, adapter integration and profiling.

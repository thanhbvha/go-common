# Transactional messaging

`db/messaging` provides a database-neutral dispatcher and typed inbox contracts.
`driver/postgres` implements PostgreSQL 16–18 and
[`driver/mysql`](driver/mysql/README.md) implements MySQL 8.4/InnoDB using the
existing ORM client;
[`driver/mongodb`](driver/mongodb/README.md) uses native MongoDB transaction scopes.
`publisher/eventbus` and `consumer/eventbus` connect durable Redis Streams, Kafka
and NATS JetStream transports.

## Packages and imports

All paths below are relative to `github.com/thanhbvha/go-common/v2/db/messaging`:

| Package | Public contracts and responsibilities |
|---|---|
| Root | `Message`, `Identity`, fingerprint, error sentinels, `Observer`, `Stats` |
| `outbox` | `Store`, `Admin`, `Publisher`, publication states, `Config`, `NewDispatcher`, `DefaultConfig` |
| `inbox` | `Receipt`, `Result`, `Applied`, `Duplicate`, typed `Processor[T]` |
| `driver/postgres` | ORM transaction binding, persistence, claims, inbox processing and migrations |
| `driver/mysql` | ORM transaction binding, InnoDB locking, persistence, inbox processing and migrations |
| `driver/mongodb` | Native MongoDB transaction binding, majority claims, inbox processing and explicit provisioning |
| `publisher/eventbus` | `NewPublisher`, publisher `Config`, stable wire identity and metadata |
| `consumer/eventbus` | `WrapInbox`, consumer `Config` and tenant resolution |
| `telemetry/otel` | OpenTelemetry observer |
| `contracttest` | Shared transaction, duplicate, fault and ownership test suite for database drivers |
| `contracttest/eventbus` | Identical Redis/NATS/Kafka crash, response-loss and DLQ replay tests for each driver |

The root, outbox and inbox packages have no database or broker imports. The two
EventBus adapters share an internal wire protocol and do not import one another.
Database transactions remain typed per driver; the inbox processor contract is
implemented by the driver so receipts and business effects commit together.

The former top-level `messaging` import path is replaced by these packages.
SQL schema, migration version, fingerprint algorithm, header names, wire IDs and
runtime behavior are unchanged by this move. Existing databases require no schema
migration for the package reorganization. Backlog admission limits and batch
dispatcher polling are separate follow-up work; workers still claim one row
only when free, and cleanup remains explicitly application-owned.

## Guarantees and boundaries

Business writes and `Store.Append` commit or roll back in the same ORM transaction.
A separate dispatcher claims committed rows, publishes outside a DB transaction,
and settles only its current, unexpired claim. Delivery is **at least once**:
broker acceptance followed by a crash, response loss or failed DB settlement can
publish again. Inbox handling commits its unique receipt and business changes
together, before returning success to EventBus.

Inbox protection lasts only while its receipt is retained. Producer request
idempotency, authorization, external-service idempotency and reconciliation of
`orm.ErrCommitUnknown` remain application responsibilities. No global or per-key
ordering is promised, including when an ordering key is forwarded to the broker.

## PostgreSQL deployment and startup

Run migrations once in a dedicated deployment job before starting replicas:

```go
files, err := postgres.MigrationFS("transactional")
if err != nil { return err }
manager, err := migration.New(migration.Options{
    DB: migrationPool, Dialect: "postgres", FS: files,
})
if err != nil { return err }
_, err = manager.Up(ctx)
```

Use a **dedicated migration metadata schema/search path** for this migration
history, or include the supplied migration in your application's complete
migration set. Goose version `202611040001` must not be independently reused for
different messaging schemas in the same metadata table. Configure the migration
pool's search path before use; session `SET search_path` on an arbitrary pooled
connection is insufficient. The example demonstrates a migration-only pgx pool.
The application pool uses fully qualified messaging table names.

Schema names accept lowercase SQL identifiers up to 63 characters. Migration
Down drops messaging tables and receipts; stop writers/dispatchers and export any
needed history before explicitly rolling it back. It leaves the schema itself.
This initial migration creates new tables; there is no legacy outbox backfill.

Application startup validates backend and schema version without creating tables:

```go
store, err := postgres.New(ctx, client, postgres.Config{
    Namespace: "orders", Schema: "transactional",
})
```

Each store has one fixed namespace. All queries include it. Tenant IDs come from
trusted application context. Nil tenant filters in administrative APIs select
all tenants in that namespace; access control belongs to the application.

## MySQL deployment and startup

Apply `mysqlmessaging.MigrationFS(database)` through `db/migration` with
`Dialect: "mysql"` and a migration-only pool selecting that database. Open the
store with `mysqlmessaging.New(ctx, client, Config{Namespace: "orders",
Database: "transactional"})`. Its Append/Consume APIs use the same
`*orm.Transaction` as PostgreSQL. MySQL 8.4/InnoDB is required; MySQL 5.7 is not
supported. DDL implicitly commits, so deploy migrations separately and reconcile
partial failures before retrying. See the [MySQL guide](driver/mysql/README.md)
for identity collation, `CLIENT_FOUND_ROWS`, snapshot and lease-clock handling.

## MongoDB deployment and startup

Run `mongodbdriver.Provision(ctx, manager, cfg)` in a deployment job, then open
with `mongodbdriver.New(ctx, manager, cfg)`. Use
`Config{Namespace: "orders", Alias: "main"}` and the existing `db/mongodb.Manager`.
Append accepts `*dbmongodb.Transaction`; inbox wrapping uses
`WrapInbox[*dbmongodb.Transaction]`. Use the driver's `TransactionOptions()`
(snapshot/majority/primary) for producer transactions. Its configured additional
aliases must share the same managed client. See the [MongoDB guide](driver/mongodb/README.md)
for provisioning, callback replay, native commit deadlines and shutdown ownership.

## SQL producer transaction

Generate IDs and serialize JSON before entering the transaction, especially for
drivers that may retry a transaction callback in future:

```go
message := messaging.Message{
    ID: uuid.NewString(), Namespace: "orders", TenantID: tenantID,
    Kind: messaging.Event, Type: "order.created", SchemaVersion: 1,
    Payload: payloadJSON,
}
err := client.WithTransaction(ctx, nil, func(tx *orm.Transaction) error {
    repo, err := orders.InTransaction(tx)
    if err != nil { return err }
    if err := repo.Create(tx.Context(), order); err != nil { return err }
    return store.Append(tx, message)
})
```

Append requires a live transaction from the store's client. Reusing the same
identity with equal immutable publication fields is an idempotent append; changed
payload, kind, type, schema version, routing, headers or schedule returns
`ErrIdentityConflict`. This does not make the preceding business mutation
idempotent: use a unique request/business key for the entire producer operation.
Commit errors preserve `errors.Is(err, orm.ErrCommitUnknown)`; reconcile by that
business key before repeating a mutation.

Payload JSON is limited to 1 MiB, headers to 128 entries / 32 KiB, and identities
to 256 UTF-8 bytes. The complete EventBus envelope may impose a smaller effective
payload budget because metadata also consumes space. `AvailableAt` controls DB
dispatch eligibility; no broker delay feature is needed for scheduled outbox rows.
EventBus also applies its envelope/header limits before broker I/O, including
metadata injected by tracing; publication errors leave the row retryable.

The inbox fingerprint hashes kind, type, schema version and normalized payload.
Object key order and whitespace do not matter; numbers use `json.Number` to keep
integers above 2^53 exact. Numeric spellings `1` and `1.0` remain distinct. Tracing,
attempts and delivery times do not change the fingerprint.

## EventBus and typed inbox

Declare the task/event topology, register handlers, and start EventBus before
running the dispatcher. The EventBus and adapter namespaces must match the store.
The adapter requires `CapabilityProvider` with broker durability; memory transports
are rejected. It forwards ordering keys only when the transport supports them.

```go
handler, err := eventbusconsumer.WrapInbox[*orm.Transaction](store,
    eventbusconsumer.Config{
        Namespace: "orders", Consumer: "billing.order-created",
        Tenant: resolveAuthorizedTenant,
    },
    func(ctx context.Context, tx *orm.Transaction, job eventbus.Job) error {
        repo, err := billing.InTransaction(tx)
        if err != nil { return err }
        // Decode and apply DB changes using tx.Context(), then optionally Append
        // another outbox event on this same tx. Do not begin a nested transaction.
        return applyBilling(tx.Context(), repo, job)
    })
```

Consumer identity is stable and specific to one logical subscriber. Reuse it
across that subscriber's replicas. Tenant resolution must validate against trusted
application policy; a message header alone does not authenticate a tenant. The
wrapper also validates namespace, tenant metadata and wire identity.

The wrapper returns success only after commit or a matching committed duplicate.
Business constraint errors, panic and cancellation roll back both receipt and
business changes. Errors and commit ambiguity remain failures for EventBus to
retry or place in its consumer DLQ. There is no separately scheduled inbox worker.

The reserved header prefix is `transactional.`. `HeaderMessageID` contains the
original business ID, `HeaderTenant` the tenant and `HeaderSchemaVersion` the
payload version. **`job.ID()` is a stable scoped wire ID**, calculated by `WireID`
from namespace, tenant, kind, type and business ID. Some brokers deduplicate IDs
automatically; this mapping prevents two tenants with the same business ID from
suppressing each other's publication. The original ID is retained for inbox keys
and replay. Use exported header constants rather than copying their strings.

## Dispatcher lifecycle and recovery

```go
publisher, err := eventbuspublisher.NewPublisher(bus, eventbuspublisher.Config{
    Namespace: "orders", // DedupeTTL: time.Hour opts into confirmed broker dedupe.
})
if err != nil { return err }
dispatcher, err := outbox.NewDispatcher(store, publisher,
    outbox.DefaultConfig())
// Supervise dispatcher.Run(runtimeCtx). Before closing its dependencies:
// dispatcher.Drain(shutdownCtx), then EventBus.Drain, then client.Close.
```

The dispatcher is single-use. It borrows dependencies and never closes them.
Run context cancellation cancels active publication. Drain first stops intake,
then waits for accepted work; an expired drain deadline cancels outstanding work.
Callbacks/publishers must honor context cancellation. Settlement gets a separate
bounded cleanup context even after a publish timeout. DB/network operations are
bounded by both dispatcher and ORM deadlines.

Defaults: concurrency 4, poll interval 1s, publication timeout 10s, settlement/claim
timeout 5s, lease 30s, 20 claim attempts, exponential retry with equal jitter from
a 1s base to a 5-minute cap. Lease must exceed publication plus settlement budgets.
Every worker claims one row only when free. There is no leased waiting queue or
heartbeat; configure larger leases/timeouts for slower broker acceptance.

State transitions:

```text
pending → processing → published
              │
              ├─ publication failure → pending (backoff) → processing
              ├─ expired lease → processing (new token)
              └─ exhausted attempts → dead → explicit Replay → pending
```

Claim commits before publishing. An ambiguous claim commit returns no claims;
ownership is recovered after lease expiry. Every claim, including one lost to a
crash, consumes an attempt. An expired final attempt is quarantined by a bounded
recovery pass. `ErrLeaseLost` means the old worker cannot settle; it does not
prove that a broker publication was prevented. Claim/settlement ambiguity never
causes a successful publication to be assumed without evidence.

Transient claim failures retry with bounded backoff; invalid configuration/schema
and recovered infrastructure panics terminate Run for lifecycle supervision.
Normal publication/settlement failures are observed and recovered through durable
state. Configuration problems such as missing topology eventually quarantine the
message; after correcting the deployment, replay it explicitly.

## Operations, retention and metrics

- `Get(tenant, id)` reads a publication; `List` uses a bounded, filter-bound cursor.
  List `State: outbox.Dead` to inspect quarantine. Cursors survive deletion but
  are not a snapshot of concurrently committing transactions.
- `Replay(tenant, ids)` changes only dead records, keeps immutable message identity,
  resets attempts and records replay count/time. It never clears inbox receipts.
- `Cleanup` is explicit and bounded per table. It requires `PublishedBefore`
  and/or `InboxBefore` cutoffs; no retention worker runs automatically. It never
  deletes pending, processing or dead outbox rows. Deleted outbox identities can
  be appended again, so retain independent producer business idempotency records.
- Choose inbox retention longer than every supported broker replay, consumer DLQ
  recovery and disaster-recovery window. Retaining inbox indefinitely is valid.
  External service idempotency keys may need a separate retention policy.
- Supply an `Observer` for publish latency, retry, quarantine, settlement errors,
  lease loss and inbox applied/duplicate/conflict events. Instrumentation panics
  are isolated from settlement. Observer callbacks must be fast/concurrency-safe.
- `telemetry/otel.New(meter)` exports bounded-label counters/histograms and backlog
  gauges. Call `store.Stats(ctx, nil)` on the application's monitoring cadence to
  sample pending count, oldest pending age, in-flight and dead rows; share the
  observer with the store and dispatcher. Use separate meters/attributes per
  store if collecting multiple stores; do not label by tenant or message ID.

Alert on growing oldest pending age, persistent claim/settlement errors, dead
records and lease loss. No payload, credential or raw driver message is included
in framework observations or printable database errors. Error chains retain
driver errors for `errors.As`; do not log unwrapped errors.

## Extending and verifying

New database drivers implement the core store/admin contracts and an
`inbox.Processor[NativeTransaction]`. Transaction binding remains typed per driver.
MySQL implements InnoDB locking reads. MongoDB implements atomic token/lease updates
and transaction-bound operations on a transaction-capable deployment. Do not put
SQL dialect switches or GORM types in the core. Run `contracttest.Run` using isolated
native fixtures for transaction, duplicate, concurrency and fencing semantics.

```bash
go test -race ./db/messaging/...
# PostgreSQL, MySQL and MongoDB with Redis/Kafka/embedded NATS and examples:
bash .github/scripts/test-messaging.sh
```

`MESSAGING_INTEGRATION_REQUIRED=1` rejects a missing PostgreSQL fixture;
`MESSAGING_MYSQL_REQUIRED=1` rejects a missing MySQL 8.4 fixture;
`MESSAGING_MONGODB_REQUIRED=1` rejects a missing MongoDB/failpoint fixture;
`MESSAGING_BROKERS_REQUIRED=1` also requires Redis/Kafka fixture addresses. The
PostgreSQL 16/17/18 CI matrix runs the storage contract and fault suite. Broker CI
uses disposable PostgreSQL/MySQL/MongoDB/Redis/Kafka containers and real JetStream,
including acceptance response loss,
crashed claims, tenant isolation, consumer retry and DLQ replay. Commit-response
loss tests commit on PostgreSQL/MySQL before suppressing the acknowledgement at the
GORM connection boundary. See the [runnable example](../../examples/messaging).

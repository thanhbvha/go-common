# MongoDB transactional messaging

This driver implements the same `outbox.Store`, `outbox.Admin` and typed
`inbox.Processor[*dbmongodb.Transaction]` contracts as the PostgreSQL driver.
It borrows an existing `db/mongodb.Manager`; no client is created or closed by
the store. It requires transaction-capable replica sets or sharded deployments
with update-pipeline support (wire version 8 or newer). The tested fixture is
MongoDB 8.0.16. Standalone deployments are rejected before provisioning.

## Provision and open

Import the database manager and messaging driver under distinct aliases:

```go
import (
    dbmongodb "github.com/thanhbvha/go-common/v2/db/mongodb"
    mongomessaging "github.com/thanhbvha/go-common/v2/db/messaging/driver/mongodb"
)

cfg := mongomessaging.Config{
    Namespace: "orders",
    Alias: "main",           // Registered manager alias; explicit and required.
    Prefix: "messaging",     // Default: messaging.
}
// Dedicated deployment job; never run this in a request or transaction callback.
if err := mongomessaging.Provision(ctx, manager, cfg); err != nil { return err }

// Application startup: read-only schema, index and capability validation.
store, err := mongomessaging.New(ctx, manager, cfg)
if err != nil { return err }
```

Provision creates `<prefix>_outbox`, `<prefix>_inbox` and `<prefix>_schema`, with
schema version 1, strict validators and unique identity indexes. Repeating the
same provision is safe; incompatible validators/versions are rejected rather
than silently upgraded. Missing required indexes may be repaired by explicitly
rerunning Provision. New never creates collections or indexes. Collection/index
collations must use binary/simple identity comparison. TTL indexes are rejected
so they cannot silently shorten inbox protection or discard undispatched events.

Prefix accepts lowercase SQL-style identifiers up to 40 characters. Namespace
and tenant identities use the shared message validation rules. Collections may
hold multiple namespaces; every operation is scoped to the configured namespace.

## Producer transaction

Generate the ID and serialize payload before entering the callback. Use the
driver's durable transaction options when writing business state and outbox:

```go
err := manager.WithTransactionAcross(ctx, []string{"main"},
    func(tx *dbmongodb.Transaction) error {
        orders, err := dbmongodb.NewTransactionRepository[Order](tx, "main", "orders")
        if err != nil { return err }
        if _, err := orders.InsertOne(tx.Context(), order); err != nil { return err }
        return store.Append(tx, message)
    }, mongomessaging.TransactionOptions())
```

`TransactionOptions()` returns fresh snapshot-read, majority-write, primary-read
options. The caller owns producer transactions; Append cannot strengthen options
chosen when the caller began the transaction. It validates that the callback is
active, declares the configured alias, and uses the same client and database.

Append preserves raw JSON payload bytes and stores headers as JSON bytes to avoid
MongoDB field-name restrictions. It uses the shared precision-preserving
business fingerprint, and verifies immutable routing, headers and schedule on
replay. Scheduling precision is one millisecond. Equal identities with different
immutable content return `messaging.ErrIdentityConflict`.

MongoDB may repeat the entire callback after a transient transaction error. Use
stable business request keys and propagate all database errors, including unique
conflicts; do not publish messages or invoke external services inside callbacks.
The framework does not retry an application business mutation outside the native
transaction API or infer success from a duplicate-key error.

## Consumer inbox and database scope

Use `consumer/eventbus.WrapInbox[*dbmongodb.Transaction]` with this store and the
same EventBus publisher adapter used for PostgreSQL. The wrapper's callback uses
`tx.Context()` for every repository operation. Returning success commits both the
receipt and business effects before EventBus can acknowledge the delivery.

For business writes across database aliases, list the additional aliases in
`Config.TransactionAliases`. The messaging alias is included automatically; all
aliases must share one managed client via `ClientGroup`. This scope applies to
Consume and framework-owned transactions. Producer callbacks declare their own
scope through `WithTransactionAcross`.

Concurrent receipt insertion can abort a MongoDB transaction. Only a conflict
from the framework's receipt write starts a fresh attempt to inspect the committed
receipt. Unique errors from business handlers propagate and roll back the receipt.
Attempts reset in-memory results before callback replay. Handler errors, panic and
cancellation never leave a successful receipt from a rolled-back transaction.

Commit errors retain native error labels through `errors.As`. Any failed or
ambiguous Consume returns an empty result and an error, allowing redelivery to
reconcile the receipt. A commit that completes after caller cancellation is also
returned as a failure; the next valid replay can return `inbox.Duplicate`.

## Dispatch, deadlines and retention

Claim uses atomic `FindOneAndUpdate` with a new token per document, server `$$NOW`
for eligibility/lease expiry, and majority-acknowledged writes. It runs outside
business transactions and never holds one open while publishing. Settlement
compares token, state, namespace and a live server-side lease. A crashed final
attempt is quarantined by a bounded exhaustion scan.

A batch contains independently committed claims. If a later claim fails or its
acknowledgement is ambiguous, Claim returns no claims and an error; already owned
documents remain recoverable after lease expiry. It does not pretend the batch
rolled back. Stable IDs and inbox receipts handle possible repeat publication.

Native operations are bounded by `Config.OperationTimeout` (default 10 seconds)
and the caller context. **The MongoDB convenient transaction API has its own
commit/retry budget based on the managed client's `OperationTimeout`.** It may
continue commit or abort cleanup after the caller deadline. Configure that client
budget to match the application's shutdown/handler policy; the store does not
mutate a shared client. Callbacks must honor cancellation. Drain dispatchers and
stop admin/native operations before disconnecting the manager. A store is bound
to one client generation and rejects a later alias rebind.

Replay, cursor pagination, statistics and explicit cutoff/batch cleanup follow
the shared API. Cursors include database, collection prefix, namespace, state and
tenant filter and are not snapshots of concurrent transactions. Replay and cleanup
use bounded native transactions; cursor results and counts are returned only after
successful completion. No retention worker or change-stream dispatcher is started.
Keep inbox receipts for the entire broker replay/DLQ/disaster-recovery window.

## Verification

The [complete runnable example](../../../../examples/messaging/README.md) uses
two database aliases and supports all durable EventBus brokers:

```bash
# Set MONGODB_INTEGRATION_URI to a disposable replica set first.
go run -race ./examples/messaging -driver mongodb -broker nats
# With explicit disposable Redis/Kafka addresses:
go run -race ./examples/messaging -driver mongodb -broker all
```

It verifies cross-database rollback/commit, lease recovery, publication response
loss, inbox replay, immutable identity conflicts and a native callback retry.

```bash
go test -race ./db/messaging/driver/mongodb
# Three-node replica set, election, native commit faults and standalone rejection:
bash .github/scripts/test-mongodb.sh replica
# TLS/authentication, native commit faults and the same storage contract:
bash .github/scripts/test-mongodb.sh secure
# PostgreSQL + MongoDB using the same real Redis/NATS/Kafka fault harness:
bash .github/scripts/test-messaging.sh
```

The database-only fixture profiles run the broker harness with embedded real
NATS. `MESSAGING_BROKERS_REQUIRED=1` selects all three brokers and fails on missing
Redis/Kafka addresses. `MESSAGING_MONGODB_REQUIRED=1` rejects a missing MongoDB URI
or failpoint fixture. All scripts own and remove their newly created fixtures;
they do not use an existing local development database.

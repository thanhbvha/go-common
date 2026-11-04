# MySQL transactional messaging

This driver implements the same outbox store/admin and typed inbox contracts as
PostgreSQL and MongoDB, using `db/orm.Transaction`. It supports **Oracle MySQL 8.4
LTS, InnoDB and strict SQL mode**, matching `db/orm/backend/mysql`. MySQL 5.7,
MariaDB and TiDB are not supported by this adapter.

## Deployment and startup

Create the deployment database first. Apply the embedded migration using a
migration-only pool whose DSN selects that database, keeping Goose history
separate from other migration sets:

```go
files, err := mysqlmessaging.MigrationFS("transactional")
if err != nil { return err }
mgr, err := migration.New(migration.Options{
    DB: migrationPool, Dialect: "mysql", FS: files,
})
if err != nil { return err }
if _, err := mgr.Up(ctx); err != nil { return err }

store, err := mysqlmessaging.New(ctx, client, mysqlmessaging.Config{
    Namespace: "orders", Database: "transactional",
})
```

Import the driver as
`mysqlmessaging "github.com/thanhbvha/go-common/v2/db/messaging/driver/mysql"`.
Database identifiers accept lowercase letters, digits and underscores, start with
an underscore/letter, and have at most 64 characters. Runtime table references are
qualified with the configured database; no pooled `USE` or session state is set.

Migration `202611040002_messaging.sql` creates `outbox`, `inbox` and
`messaging_schema`. **MySQL DDL implicitly commits**: apply it through one
supervised deployment job. A partial DDL failure requires inspection/reconciliation
before rerunning; the schema version marker is inserted only after all tables
exist. Startup never migrates. It checks the version, columns, InnoDB engines,
binary identity columns, unique identities and required claim/cleanup indexes.
Down drops these three tables and leaves the database; stop writers first and
preserve any history that must survive rollback.

## Transaction binding

```go
// Generate message ID and serialize JSON before entering the transaction.
err = client.WithTransaction(ctx, nil, func(tx *orm.Transaction) error {
    if err := writeBusinessState(tx); err != nil { return err }
    return store.Append(tx, message)
})

result, err := store.Consume(ctx, receipt, func(tx *orm.Transaction) error {
    // Receipt and these business writes commit or roll back together.
    // Append another outbox publication through this same transaction if needed.
    return applyProjection(tx)
})
```

Append rejects transactions from another ORM client or a completed callback.
Producer replay checks all immutable content, including routing, headers and
schedule, using a locking read. It remains correct if the caller already created
a `REPEATABLE READ` snapshot before another producer committed the same ID.

Inbox uses `READ COMMITTED`, an insert-if-absent operation and a random insertion
token. It does not infer insertion from affected rows: the ORM intentionally uses
`CLIENT_FOUND_ROWS`, under which a duplicate no-op can report one matched row.
Only the token owner executes the callback. A committed matching receipt returns
`inbox.Duplicate`; changed fingerprints return `messaging.ErrIdentityConflict`.
Business unique violations remain errors. Panic/error/cancellation roll back
receipt and effects. Commit uncertainty preserves `orm.ErrCommitUnknown` and
never returns a successful inbox result or an ambiguous claim batch.

## Claims and operations

Claims use short `READ COMMITTED` InnoDB transactions and separate index-ordered
`FOR UPDATE SKIP LOCKED` scans for expired processing rows and due pending rows.
There is no database transaction open while the broker accepts publication.
Claims receive fresh tokens, increment attempts and use the database clock for
lease expiry. Settlement acquires its row lock **before** checking current DB
time, so waiting for a lock cannot authorize settlement of an expired lease.
Crash recovery, retries, exhaustion, quarantine and replay use the common core.

Identities use `VARBINARY(256)` to preserve case/accent distinctions independently
of the deployment's text collation. JSON payload bytes remain in `MEDIUMBLOB`;
headers use JSON. Runtime timestamps use UTC `DATETIME(6)` and `UTC_TIMESTAMP(6)`.
DSN `loc` and server session `time_zone` do not change schedule/lease semantics.
The original schedule is retained at microsecond precision as RFC3339; schedules
before DATETIME's year-1000 minimum remain representable and are already due.

`Get`, cursor-based `List`, `Replay`, bounded `Cleanup`, `Stats` and `Observer`
match the other drivers. Cleanup has no automatic retention defaults and never
removes pending, processing or dead publications. Keep inbox receipts longer than
the supported broker replay/DLQ recovery period. Producer business idempotency,
backlog monitoring and reconciliation after unknown commits remain application
responsibilities. Clients and brokers are application-owned.

## Example and verification

Set `mysql.dsn` in [`examples/messaging/config.yaml`](../../../../examples/messaging/config.yaml)
to an existing disposable MySQL 8.4 database, then run from the repository root:

```bash
go run ./examples/messaging -driver mysql -broker redis
# -broker nats runs an owned embedded JetStream server.
```

`ORM_MYSQL_DSN` overrides YAML. The example creates and drops only its uniquely
named database, so its user needs CREATE/DROP/database DDL permissions. Run the
entire isolated three-database/three-broker suite without configuring services:

```bash
bash .github/scripts/test-messaging.sh
```

The driver runs `contracttest.Run` and the identical EventBus fault suite for
Redis, Kafka and NATS. Additional tests cover native business constraint errors,
commit-response loss, preexisting producer snapshots, binary identities, UTC
handling, lock-wait lease expiry, schema rejection and migration Up/Down/reapply.
Required fixture mode is `MESSAGING_MYSQL_REQUIRED=1` with `ORM_MYSQL_DSN`.

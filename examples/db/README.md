# Complete db/orm examples

The program provides **14 independently runnable demos** using the same API on
SQLite, PostgreSQL, and MySQL. It defaults to in-memory SQLite and requires no
database server, Redis, or OpenTelemetry collector.

Each demo creates a random table namespace, `ormdemo_<hex>_`, writes only synthetic
data, and attempts to drop those tables before closing the pool. It does not modify
the application's `patients` table. Use a dedicated test database: the demos create
and drop tables, deliberately trigger constraint errors, and require DDL privileges.
If the process is killed, prefixed tables may remain; clean up only the namespace
belonging to that terminated run.

## Run from the repository root

```sh
go run ./examples/db -list
go run ./examples/db
go run ./examples/db -demo crud
go run ./examples/db -demo scope,transactions,cas
go run ./examples/db -demo all -otel
```

| Flag | Default | Description |
| --- | --- | --- |
| `-backend` | `sqlite` | `sqlite`, `postgres`, or `mysql` |
| `-demo` | `all` | One demo name or multiple comma-separated names |
| `-otel` | `false` | Enable local trace/metric SDKs for every selected demo |
| `-list` | `false` | List demos and exit without connecting to a database |

Selected demos run in registry order, each with its own data schema. The `telemetry`
demo always enables local SDKs, even without `-otel`. The program has an overall
two-minute deadline; cleanup uses a separate five-second context.

### PostgreSQL 16–18

Supply `ORM_POSTGRES_DSN` through the environment, then run:

```sh
go run ./examples/db -backend postgres -demo all
```

The test account needs permission to create and drop tables, indexes, and foreign
keys in the selected database. Include the environment's TLS/auth configuration in
the DSN; the program neither prints the DSN nor automatically disables TLS. Keep
real credentials out of source code, command history, and committed files.

### MySQL 8.4 / InnoDB

Supply `ORM_MYSQL_DSN` through the environment, then run:

```sh
go run ./examples/db -backend mysql -demo all
```

The database must use InnoDB and strict SQL mode. The adapter rejects visible
non-InnoDB base tables in the database, as well as the `multiStatements` and
`interpolateParams` options. It preserves configured TLS/auth settings and enables
`parseTime` and `clientFoundRows` to maintain the update contract.

The backend is selected explicitly through the flag. DSNs for other backends are
not used. There is no need to create the Patient/Visit schema beforehand: the
runner creates **prefixed scratch tables**. This is an example fixture mechanism,
not a production migration strategy.

## Demos and expected results

| Demo | Features exercised | Main result |
| --- | --- | --- |
| `backend` | DefaultConfig, Validate, adapter factory, Requirements.Check, Capabilities, SupportsIsolation | Print backend/version/budgets and detect unsupported requirements |
| `crud` | Create, CreateMany, FindByID, FindOne, PatchByID, DeleteByID, RestoreByID | Patch `0`, `false`, and empty strings; hide soft-deleted records and restore them |
| `pagination` | PageRequest/Response, Sort, Ascending/Descending, Scope filter, CursorRequest/Response | Total=5, TotalPages=3; cursor traversal visits all 5 records in either direction |
| `scope` | RepositoryOptions.Scope, tenant predicate, InTransaction | Cross-tenant reads/patches/restores return ErrNotFound; transaction binding preserves policy |
| `upsert` | UpsertMany, UpsertPolicy, conflict target, DoNothing, AnyUniqueKey | PostgreSQL/SQLite use strict targets; MySQL requires explicit AnyUniqueKey selection |
| `transactions` | WithTransaction, sql.TxOptions, Context, InTransaction, NewTransactionRepository, Transaction.DB | Verify commit, rollback, panic rollback, and context/nested/expired guards |
| `cas` | VersionColumn, CompareAndSwapByID, ErrVersionRequired, ErrVersionConflict | Version increases from 1 to 2; a writer using the stale version is rejected |
| `limits` | BatchSize, MaxBulkRows, MaxPageSize, MaxOffset, byte budget, sort/cursor validation | Reject oversized input before generating write SQL |
| `manager` | NewManager, Init, default/named Lookup, Add, Close | 3 independent pools; detect duplicate/missing/reinit/closed and wrong-client usage |
| `admission` | MaxInFlight, MaxQueued, ErrBusy, cancellation/deadline, AdmissionStats | Reject a full queue; cancel 2 queued operations; finish with in_flight=queued=0 |
| `lifecycle` | Open, Ping, Close with deadline, drain/retry/idempotency, TransactionStats | A short Close hits its deadline; the active transaction completes and a subsequent Close succeeds |
| `native` | Client.DB, parameterized Raw, aggregate, join | 1 aggregate group, raw_count=2, and 1 joined row |
| `errors` | errors.Is/As, Error metadata/Unwrap, native driver fields, Backend.Classify | Distinguish not-found, validation, unique/FK, cancellation, and database errors without exposing payloads |
| `telemetry` | Logger, EnableTelemetry, instance-owned providers, Monitor, Stats | Produce spans/metrics/events; a synthetic sensitive marker does not appear in spans |

The `ERROR orm query failed` lines in the `errors` demo are intentional. A demo
succeeds only when errors match the expected types; unexpected errors produce a
nonzero exit code. Successful execution ends with:

```text
All selected demos completed.
```

IDs, span counts, instrument counts, and durations may vary by backend/version.
They are neither benchmarks nor production latency guarantees.

## Source layout

| File | Contents |
| --- | --- |
| [main.go](main.go) | Flags, demo registry, and program lifecycle |
| [setup.go](setup.go) | Models, configuration, adapters, namespaces, fixtures, and cleanup |
| [repositories.go](repositories.go) | Backend, CRUD, pagination, scope, upsert, CAS, and limits |
| [transactions.go](transactions.go) | Commit/rollback, low-level transaction repositories, and settlement error handling |
| [operations.go](operations.go) | Manager, admission, and graceful shutdown |
| [native_errors.go](native_errors.go) | Native GORM, raw/aggregate/join queries, and error-chain checks |
| [telemetry.go](telemetry.go) | Local SDK providers, Monitor, and metric snapshots |

## Connection and backend configuration

`newConfig` in setup.go demonstrates all operational fields:

| Group | Fields | Demo configuration |
| --- | --- | --- |
| Driver | Backend | sqlite.New / postgres.New / mysql.New, selected by flag |
| Required features | Require | Requirements; the backend demo checks server timeout policy |
| Pool | MaxOpenConns / MaxIdleConns | 2 / 1; SQLite uses 1 writer |
| Connection lifetime | ConnMaxLifetime / ConnMaxIdleTime | 30 minutes / 5 minutes |
| Startup/query/transaction | StartupTimeout / QueryTimeout / TransactionTimeout | 5s / 5s / 10s |
| Admission | MaxInFlight / MaxQueued | 2 / 8; SQLite uses 1 in-flight; the admission demo uses 1 / 1 |
| Logging | Logger / SlowThreshold | slog.Default / 200ms |
| Observability | EnableTelemetry / TracerProvider / MeterProvider / Monitor | Per-client SDKs when telemetry is enabled |

Do not copy the demo budgets directly into production. Count connections across
all replicas/pools targeting the same server, reserving capacity for migrations
and administration.

```go
cfg := orm.DefaultConfig()
cfg.Backend = postgres.New(os.Getenv("ORM_POSTGRES_DSN")).WithTimeouts(
    postgres.Timeouts{
        Statement:         10 * time.Second,
        Lock:              time.Second,
        IdleInTransaction: 30 * time.Second,
    },
)
cfg.Require = backend.Requirements{
    ReadOnlyTransactions:     true,
    TransactionLocalTimeouts: true,
}
if err := cfg.Validate(); err != nil {
    return err
}
client, err := orm.Open(ctx, cfg)
```

`postgres.DefaultTimeouts()` returns the default policy; WithTimeouts returns a
copy of the adapter. A zero value inherits the server setting rather than disabling
an existing limit. PostgreSQL applies transaction-local settings and preserves
stricter server limits. MySQL/SQLite do not emulate that policy: when Require
requests it, Validate/Open returns ErrUnsupportedFeature. Client-side QueryTimeout
and TransactionTimeout still apply and require context-aware code; they cannot
stop a Go callback that refuses to return.

`client.Capabilities()` is the profile verified against the server, whereas
`cfg.Backend.Capabilities()` is the factory profile before connection. The profile
includes version, transactions/savepoints, matched rows, upsert modes, read-only
support, isolation, and bind/byte budgets. Call `SupportsIsolation(sql.Level...)`
before choosing an isolation level.

## Repositories and limits

```go
opts := orm.RepositoryOptions{
    WritableColumns: []string{"name", "age", "active", "status"},
    SortableColumns: []string{"age", "name"},
    BatchSize:       2,
    MaxBulkRows:     20,
    MaxPageSize:     10,
    MaxOffset:       100,
}
repo, err := orm.NewRepository[Patient](client, opts)
```

Column names are database identifiers, not SQL fragments. IDs are bound and
normalized to the primary-key type; MySQL cannot coerce `"1suffix"` into ID 1.
Patch accepts maps containing supported scalar, NULL, time, byte, and driver.Valuer
values; it does not accept gorm.Expr from external input. Create/CreateMany skip
associations; the Visit relationship is created explicitly inside a transaction.

CreateMany/UpsertMany split batches by row count, bind budget, and estimated bytes;
all batches participate in one transaction. On failure, discard in-memory IDs/model
mutations from that write attempt. MaxBulkRows limits row count and does not replace
body-size and field-length limits at the HTTP/consumer boundary.

Patient has gorm.DeletedAt, so DeleteByID performs a soft delete. For a model without
that marker, DeleteByID physically deletes the row. RestoreByID supports only the
standard marker and preserves Scope. VersionColumn applies only to CAS patches;
Delete/Restore do not automatically check versions.

### Filters, tenants, and pagination

```go
opts.Scope = func(db *gorm.DB) *gorm.DB {
    return db.Where("tenant_id = ?", authenticatedTenantID)
}
repo, err := orm.NewRepository[Patient](client, opts)
page, err := repo.Paginate(ctx, orm.PageRequest{
    Page: 1, Size: 10,
    Sorts: []orm.Sort{{Field: "age", Direction: orm.Descending}},
}, func(db *gorm.DB) *gorm.DB {
    return db.Where("status = ?", "waiting")
})
```

Scope/filter callbacks are trusted server code; input values belong in placeholders.
Derive the tenant from authenticated identity, not a self-declared payload. A WHERE
scope does not validate ownership during INSERT; the application must validate and
assign TenantID before Create.

Page/Size=0 selects defaults; sizes/offsets above the limits are rejected. Sort uses
an allowlist, direction is typed, and the primary key is added as a tie-breaker.
Count and page retrieval are separate statements; use a repeatable-read transaction
when a consistent snapshot is required. CursorPage traverses the unique primary key,
returns Items and Next, and does not run COUNT. Keep tenant/filter/direction unchanged
between pages. A cursor represents a position, not access permission.

### Upsert by capability

```go
policy := orm.UpsertPolicy{
    ConflictColumns: []string{"external_id"},
    UpdateColumns:   []string{"name", "status"},
}
// PostgreSQL/SQLite: handle only this specific unique target.
rows, err := repo.UpsertMany(ctx, models, policy)

// MySQL: any unique key can trigger UPDATE; explicitly opt into this behavior.
policy = orm.UpsertPolicy{
    AnyUniqueKey:  true,
    UpdateColumns: []string{"name", "status"},
}
```

MySQL supports neither DoNothing nor strict targets. There is no default UpdateAll.
Upsert is rejected on repositories with Scope or VersionColumn; use a dedicated
business query with a conflict predicate that enforces tenant isolation and an
appropriate version policy. Upsert RowsAffected is the driver-reported value, not
a portable logical record count.

### Optimistic locking

```go
opts.VersionColumn = "version"
repo, err := orm.NewRepository[Patient](client, opts)
err = repo.CompareAndSwapByID(ctx, id, expectedVersion,
    map[string]any{"status": "ready"})
```

The version column must not appear in WritableColumns. CAS checks ID + Scope +
version and increments the version in one UPDATE. Zero matched rows returns
ErrVersionConflict, which also matches ErrConflict, even when the row is missing
or invisible to the caller. No existence probe exposes another tenant's data.
PatchByID and generic upsert are disabled when this policy is enabled.

## Transactions and multiple databases

```go
err := client.WithTransaction(ctx,
    &sql.TxOptions{Isolation: sql.LevelRepeatableRead},
    func(tx *orm.Transaction) error {
        bound, err := repo.InTransaction(tx)
        if err != nil { return err }
        return bound.PatchByID(tx.Context(), id,
            map[string]any{"status": "ready"})
    })
```

InTransaction preserves the root repository's Scope, allowlists, version policy,
and limits, and verifies client ownership. Always pass tx.Context or a child context.
The following are rejected: passing a transaction context to a root repository,
passing an unrelated context to a transaction repository, nested transactions, and
using a transaction repository after its callback has finished.

NewTransactionRepository is a low-level constructor for a **new policy** inside the
callback; it does not infer an existing policy. The example uses it for a new Visit,
while Patient always binds from the root repository. Use Transaction.DB only inside
the callback; do not commit, roll back, or nest transactions manually.

Manager can contain different backends:

```go
primary := orm.DefaultConfig()
primary.Backend = postgres.New(os.Getenv("ORM_POSTGRES_DSN"))
reporting := orm.DefaultConfig()
reporting.Backend = mysql.New(os.Getenv("ORM_MYSQL_DSN"))
m := orm.NewManager()
err := m.Init(ctx, map[string]orm.Config{
    "primary": primary, "reporting": reporting,
}, "primary")
```

The manager demo uses three pools of the selected backend, allowing a fully offline
run with SQLite. Pools/namespaces are distinct but may share a server. Transactions
are not atomic across pools/databases. Use outbox/idempotency/saga patterns as the
business workflow requires when writing to multiple destinations. MySQL transactions
guarantee only InnoDB DML; DDL can implicitly commit, so run migrations separately.

### Transaction errors and retries

`errors.As(err, &transactionError)` exposes Phase, Outcome, and RollbackError().
Phases are admission/begin/setup/callback/commit; outcomes are not_started/
rolled_back/committed/unknown. `describeError` in transactions.go demonstrates safe
handling.

ErrCommitUnknown can occur after data has committed. The runner does not deliberately
break the environment by dropping the network/commit acknowledgement; the handling
branch is provided, while fault injection belongs in the module tests. Do not retry
blindly. Deadlock/serialization errors require an explicit idempotent policy for
retrying the entire transaction; assess lock/statement timeouts in the context of
contention and query cost. Panic rollback is demonstrated with a local recover.

## Admission and shutdown

A transaction holds one admission permit throughout its callback and settlement;
statements inside it do not acquire additional permits. A full queue returns ErrBusy,
and queued operations honor cancellation and deadlines. Stats separate admission
waits from SQL pool waits. MaxQueued=0 rejects immediately when permits are exhausted;
strict FIFO ordering is not guaranteed.

Close is terminal and idempotent. Close with a short context only limits waiting;
it does not terminate Go callbacks. Once a cooperative callback returns, call Close
again to await completion. Do not call Close inside a transaction or Monitor callback.
The lifecycle demo creates no tables, allowing terminal shutdown to be tested safely.

## Errors and native SQL

| Group | Sentinels/metadata |
| --- | --- |
| Queries/writes | ErrNotFound, ErrConflict, ErrDatabase |
| Input/configuration | ErrInvalidArgument, ErrInvalidConfig, ErrUnsupportedFeature, ErrUnsupportedVersion |
| Lifecycle/manager | ErrClosed, ErrAlreadyInitialized, ErrDuplicateConnection |
| Admission/context | ErrBusy, context.Canceled, context.DeadlineExceeded |
| Transaction scope | ErrNestedTransaction, ErrTransactionContext, ErrTransactionClosed, ErrTransactionClientMismatch |
| Settlement | ErrCommitUnknown, TransactionError.Phase/Outcome/RollbackError |
| Optimistic locking | ErrVersionRequired, ErrVersionConflict |
| Backend concurrency | ErrDeadlock, ErrSerialization, ErrLockTimeout, ErrStatementTimeout |

The errors demo demonstrates errors.Is/As and Error.Operation/Kind/SQLState/DriverCode.
Native driver fields are inspected by type without printing Message/Detail.
Error.Unwrap is used only for structural checks, not for logging raw causes.
Backend.Classify returns metadata; the core invokes Prepare/Inspect/SetupTransaction,
so services do not need to call them manually. ErrUnsupportedVersion occurs when
startup encounters an unsupported server version; the runner does not change server
versions to manufacture that error.

Client.DB is an escape hatch: it does not automatically apply Scope, allowlists,
admission, or repository limits. native_errors.go demonstrates explicit deadlines,
value placeholders, and clause.Table for application-approved identifiers. Never
concatenate request input into Raw, Order, Select, Group, or Joins. Native GORM
returns raw errors; sanitize them before logging. Aggregate/join demos print only
synthetic row counts, not payloads.

## Logging, tracing, and metrics

telemetry.go creates instance-owned TracerProvider, MeterProvider, and ManualReader
objects without changing global providers or requiring a collector. `-otel` reuses
this wiring for every demo. Real services can supply OTLP-configured providers
through the same Config.

Monitor receives EventKind/Event.Phase/Outcome/Duration/WaitDuration; its callback
must be fast, thread-safe, and must not invoke lifecycle methods. The example only
increments an atomic counter. Stats(), AdmissionStats(), and TransactionStats()
report pool, queue, and transaction outcomes.

Metrics include orm.admission.wait/rejected, orm.transaction.duration/transactions,
and orm.pool.connections/waits/wait_time. Pool metric callbacks are unregistered
when the client closes. Close the client before shutting down SDK providers.
Loggers/spans do not record SQL, parameter values, DSNs, or exception payloads. The
demo verifies that a synthetic sensitive marker does not appear in spans.

## Namespaces, migrations, and backend extension

namespacedBackend in setup.go is a decorator: it forwards Name/Validate/Capabilities/
Inspect/SetupTransaction/Classify, wraps Prepare to add NamingStrategy, and forwards
PreparedPool ownership when the adapter supplies a pool before GORM initialization.
It demonstrates interface extension without global registration. It neither replaces
the driver nor relaxes feature guards.

AutoMigrate is used only for the runner's scratch schema. Production applications
should use fixed table names and versioned SQL migrations for each backend, rather
than running AutoMigrate when opening pools. The sql/ directory retains the older
Goose/SQLite example that creates example_users; the new runner does not call it,
and it is not the schema used by these demos. See the
[ORM guide](../../db/orm/README.md) and [db/migration](../../db/migration).

## Verification

```sh
go test ./examples/db
go vet ./examples/db
go run -race ./examples/db -demo all -otel
go run ./examples/db -backend postgres -demo all
go run ./examples/db -backend mysql -demo all
```

The last two commands require environment DSNs and the test databases described
above. ORM CI runs the SQLite, PostgreSQL, and MySQL examples alongside the contract
suite. These are API examples with result assertions and cleanup; application-specific
TLS/failover, workloads/SLOs, and business schemas still need staging verification.

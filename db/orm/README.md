# ORM

Instance-owned SQL pools and a deliberately small GORM repository layer. There
are no package globals, implicit migrations, automatic write retries or hidden
update-to-insert fallbacks. Built-in adapters cover PostgreSQL 16–18, Oracle MySQL 8.4 LTS with InnoDB, and
SQLite through the bundled pure-Go driver (SQLite 3.35+). Support means a shared
behavioral contract plus explicit capability differences, not universal SQL
compatibility. Other engines need their own adapter and contract verification.

## Open and own a pool

```go
cfg := orm.DefaultConfig()
cfg.Backend = postgres.New(os.Getenv("DATABASE_DSN")) // db/orm/backend/postgres
cfg.Logger = slog.Default()
cfg.EnableTelemetry = true // Uses the application's OTel provider by default.
client, err := orm.Open(ctx, cfg)
if err != nil {
    return err
}
defer func() {
    shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    // Propagate/report this error in your application shutdown handler.
    _ = client.Close(shutdown)
}()
```

`Open` owns the pool it creates. Do not supply an externally owned `sql.DB` via a
dialector. The default limits are 20 open / 5 idle connections, 30-minute maximum
lifetime, 5-minute maximum idle time, 5-second startup timeout, 10-second query
timeout, and 30-second transaction timeout. `Config{Backend: ...}` alone is
invalid: start with `DefaultConfig()` and validate changes. Zero idle connections
is supported; zero maximum open connections is rejected. Tune the aggregate
budget across all replicas against each database server's connection budget.

TLS and authentication are controlled by the deployment's driver configuration.
The wrapper never turns TLS verification off. Do not print or commit DSNs. The
startup context covers backend preparation, PingContext and capability inspection.
Custom adapters must avoid unbounded I/O in Prepare/Initialize. GORM's implicit
startup ping is disabled. Foreign-key migration defaults remain enabled.

For named pools, use `NewManager()`, `Init(ctx, configs, defaultName)`,
`Lookup(name)` and `Add(ctx, name, cfg)`. Empty lookup name selects the default.
A single config can infer its default; multiple configs require an explicit
default. Init validates all configs before opening any pool, rolls back newly
opened pools on failure, and publishes them together. A repeated successful Init
returns `ErrAlreadyInitialized`; Add never overwrites an alias. Managers are
independent; lookups do not wait for another alias's network connection attempt.

`Close(ctx)` is terminal and idempotent. New managed work is rejected while
existing managed transaction callbacks drain. A timeout bounds the caller's wait,
not the callback's execution: callbacks must honor their context and return. A
subsequent Close can finish waiting. Never call Close inside a transaction
callback. New deployments create a fresh manager/client rather than reopening a
closed instance. Ordinary queries already running are handled by `sql.DB.Close`;
managed repository operations and transactions are admitted and drained together.

Each client has bounded admission. MaxInFlight=0 derives its limit from
MaxOpenConns; MaxQueued defaults to 100. Set MaxQueued=0 to reject immediately
when all permits are occupied. Excess work returns ErrBusy. Queue waits count
against QueryTimeout/TransactionTimeout, honor cancellation and wake on Close.
A transaction holds one permit through begin, callback and settlement; its
repository statements do not acquire another permit. Waiting work is bounded,
but strict FIFO scheduling is not promised. Native DB calls bypass this gate.

Use `Ping(ctx)` for dependency readiness and `Stats()` for pool metrics, including
InUse, Idle, WaitCount, WaitDuration, and connection retirement counts. A database
outage should affect readiness rather than blindly restart every application
replica. Register database startup before the HTTP component and stop incoming
traffic before closing the pool. For `foundation.Component`, Start opens the
manager and returns; Stop calls manager.Close with the shutdown context. The
framework stops components in reverse registration order.

## Backend selection and capabilities

Import an adapter from db/orm/backend/postgres, db/orm/backend/mysql or
db/orm/backend/sqlite, then assign Config.Backend. Adapters implement the shared
backend.Backend interface; there is no global registration or automatic fallback.
All configurations remain in the same Go module.

~~~go
cfg := orm.DefaultConfig()
cfg.Backend = mysql.New(os.Getenv("DATABASE_DSN")) // db/orm/backend/mysql
cfg.Require = backend.Requirements{ReadOnlyTransactions: true}
client, err := orm.Open(ctx, cfg)
if err != nil {
    return err
}
caps := client.Capabilities() // Verified version, feature flags and batch budgets.
_ = caps
~~~

| Contract | PostgreSQL 16–18 | MySQL 8.4/InnoDB | SQLite bundled driver |
| --- | --- | --- | --- |
| Transactions / internal savepoints | Yes | Yes | Yes |
| Matched-row patch contract | Native | Enforced clientFoundRows | Native |
| Strict upsert conflict target | Yes | Rejected | Yes |
| Exact conflict DoNothing | Yes | Rejected | Yes |
| Explicit AnyUniqueKey upsert | Rejected | Yes | Rejected |
| Read-only transaction | Yes | Yes | Rejected |
| Transaction-local server timeout policy | Yes | Rejected when required | Rejected when required |
| Isolation levels | Driver-supported SQL levels | Driver-supported SQL levels | Default / Serializable |

Unsupported requirements/options return ErrUnsupportedFeature instead of silently
weakening guarantees. Open rejects unverified PostgreSQL major versions and
MySQL variants/versions outside the declared target. MariaDB, TiDB, SQL Server,
Oracle and ClickHouse require separate adapters and CI contracts.

The MySQL adapter parses DSNs without exposing credentials, preserves configured
TLS/authentication, enforces parseTime=true and clientFoundRows=true, and rejects
multiStatements/interpolateParams. It suppresses raw driver diagnostic output.
It skips GORM's unbounded SELECT VERSION() initializer, then probes version,
strict SQL mode, InnoDB and packet limits with the overall startup context. The
adapter rejects existing non-InnoDB base tables visible in the selected database.
Future migrations must retain InnoDB and strict SQL mode; application accounts
should not have privileges to change those guarantees or access unrelated DBs.
No session variables are changed to emulate PostgreSQL timeouts: MySQL server
timeouts belong in deployment policy, and max_execution_time is not a general
DML timeout. Client query/transaction deadlines still apply.

MySQL transaction guarantees cover InnoDB DML. DDL can cause implicit commits;
run schema changes in the dedicated migration workflow rather than inside
WithTransaction. A transaction still belongs to one client/pool across all backends.

The SQLite adapter enables foreign keys and a bounded busy timeout on every
connection via owned DSN pragmas. User-supplied _pragma parameters are rejected.
Prefer one writer connection for SQLite and test file locking under the intended
deployment. SQL typing, collation, timestamp precision and schema migrations remain
backend-specific: choose explicit lengths/collations for identifiers and unique
keys, and maintain versioned migrations for each engine. Do not assume a model
with vendor-specific defaults/index expressions is portable.

Custom adapters are trusted code. They must preserve bound values, matched-row
semantics, isolation/read-only requirements, safe error metadata and context-aware
startup. Their profile is immutable per client. Classify returns metadata while
the core preserves the native error chain. Add a real-database contract suite before
advertising another backend.

## Repository contracts

```go
opts := orm.RepositoryOptions{
    WritableColumns: []string{"name", "active"},
    SortableColumns: []string{"created_at"},
}
repo, err := orm.NewRepository[User](client, opts)
if err != nil {
    return err
}
err = repo.PatchByID(ctx, id, map[string]any{"active": false, "name": ""})
```

Models must be structs with one integer, unsigned integer or string primary key.
Composite keys and pointer type parameters are rejected. ID arguments are
nonempty strings or positive integers, including driver.Valuer implementations
that produce those scalar types. IDs are always bound values; SQL expressions,
maps and slices cannot become ID predicates. Custom primary-key column names
come from GORM schema metadata.

Numeric key strings are parsed strictly into the model's integer type before
binding; malformed values such as "1suffix" are rejected rather than depending
on a database's implicit coercion. String primary keys require string values.

Column options use database column names, not SQL fragments or Go field names.
Writable columns are explicit. Primary keys, GORM timestamps and the soft-delete
marker cannot be patched. Patch accepts scalar values, time.Time, byte slices,
NULL and scalar driver.Valuer results, and rejects executable GORM expressions.
It updates supplied zero values. Pointer/struct patch objects are deliberately
not accepted; encode optional fields into the map explicitly.

| Operation | Contract |
| --- | --- |
| Create | Insert one model; never updates on conflict; associations omitted |
| CreateMany | Bounded SQL batches in one transaction; associations omitted |
| PatchByID | Update supplied allowlisted columns; never inserts |
| DeleteByID | Soft delete when model supports it; otherwise physical delete |
| RestoreByID | Restore only deleted rows with standard `gorm.DeletedAt` |
| UpsertMany | Explicit conflict target and update columns, or DoNothing |
| FindByID / FindOne | Return `ErrNotFound` when no row matches |
| Paginate / CursorPage | Bounded reads with deterministic ordering |

Patch/Delete/Restore return `ErrNotFound` on zero affected rows. PostgreSQL and SQLite count matched updates; the MySQL adapter enforces
clientFoundRows=true so an unchanged existing row is not reported missing. Restore uses the actual tagged
soft-delete column and retains repository scopes. It does not restore associated
models. There is no unscoped generic repository or automatic hard-delete helper.

Strict-target upsert requires a real unique constraint matching ConflictColumns
and an explicit choice of update columns. UpdateAll is unavailable. DoNothing can return zero
affected rows successfully on PostgreSQL/SQLite. MySQL rejects strict conflict
targets and DoNothing with ErrUnsupportedFeature. Its explicit AnyUniqueKey=true
mode uses ON DUPLICATE KEY UPDATE and requires update columns; any unique key can
trigger it. This mode cannot include ConflictColumns or DoNothing. Upsert row
counts remain driver-native, not a portable count of logical records. Scoped repositories reject generic upsert because
ON CONFLICT does not enforce an ordinary WHERE authorization predicate. Implement
tenant-specific upsert using the native transaction API with tenant-safe unique
constraints and a conflict predicate. Do not retry an ambiguous commit blindly.

BatchSize defaults to 100 and is limited to 1..1000; it is lowered automatically
for wide models to stay within the verified backend bind-variable budget.
Bulk statements are also split using a conservative payload/SQL byte estimate: 4MiB
for PostgreSQL, 1MiB for SQLite, and at most 1MiB/half the packet limit
for MySQL. Oversized individual rows are rejected before SQL. Custom Valuers and
serializers must be pure; hooks are trusted and can change values after estimation,
so the driver remains the final packet-size guard.
CreateMany and UpsertMany are
atomic across batches. Ignore generated IDs/model mutations after any error;
in-memory values cannot be rolled back. Unbounded FindAll, Distinct, Pluck,
RawQuery and Aggregate wrappers were removed. Use typed application repositories
with explicit query limits for those workloads. Large imports should split input
upstream and choose transaction sizes appropriate to the job. MaxBulkRows defaults
to 10,000, with an explicit configurable ceiling of 1,000,000. Oversized slices
are rejected before admission or SQL. Identity prevalidation runs inside the
admitted query deadline and checks cancellation for every row before any batch
is issued. This bounds rows, not arbitrary bytes within a field: apply request
body and domain field-size limits before decoding large inputs.

## Optimistic concurrency

Set RepositoryOptions.VersionColumn to an integer model column to require CAS
patches. The version column cannot also be writable. Then call
`repo.CompareAndSwapByID(ctx, id, expectedVersion, patch)`. One UPDATE checks the
ID, tenant Scope and expected version, applies the allowlisted patch and increments
the version. Concurrent writers with the same expected version have one winner.
Version starts from the model/database's initial value (zero is supported).

Zero matched rows returns ErrVersionConflict (also errors.Is(err, ErrConflict)),
including missing or inaccessible rows; no separate existence check leaks tenant
information. Integer overflow is rejected. PatchByID and generic UpsertMany are
disabled on versioned repositories with ErrVersionRequired. DeleteByID and
RestoreByID retain their ID-based contracts and do not check versions; implement
version-aware deletion in the application repository when required. This policy
does not constrain other services or native SQL writers: they must follow the same
version contract or use database constraints/locking appropriate to the domain.

## Filtering, tenancy and native access

`RepositoryOptions.Scope` is trusted, concurrency-safe application code applied
to every repository query. It can add tenant/ownership predicates. Method filter
arguments are also trusted Scope functions; external values belong in GORM
placeholders. Filters for pagination must add predicates only, without replacing
model/table, projections, groups, limits or order.

A generic repository is not an authorization boundary. Authorize the request and
validate inserted ownership fields in application code. A WHERE scope does not
authorize inserted data. Do not bind request payloads directly into database
models with privileged fields. Native `Client.DB()` and `Transaction.DB()` are
explicit escape hatches for migrations, joins, aggregates, locks and hard delete;
they bypass repository limits and guards. GORM query builders, custom hooks and
driver.Valuer implementations are trusted code. Never concatenate external SQL
or dynamic identifiers; choose identifiers from application allowlists.

## Pagination

Page/Size zero select page 1 and size 10. Invalid/oversized values are rejected,
not silently expanded. MaxPageSize defaults to 100 (configurable up to 1000);
MaxOffset defaults to 100,000 (up to 1,000,000). Offset multiplication is checked
before execution. At most eight distinct sort fields are accepted; each field
must be sortable and each direction must be `orm.Ascending` or `orm.Descending`.
The unique primary key is appended as a tie-breaker when absent.

Count and page reads are independent statements. Use a repeatable-read
transaction when they must see one snapshot. CursorPage avoids count/deep offset
queries and traverses the primary key in either direction. It reads at most
Size+1 records and returns a Next cursor only when more rows exist. Cursors are
versioned, length-bounded and tied to model/key/direction. They are positions,
not secrets or authorization grants; preserve tenant scope and filters between
pages. Concurrent inserts/deletes can still change the traversal. Choose immutable
primary keys and indexes for the actual filter/order workload.

## Transactions and errors

```go
err := client.WithTransaction(ctx, &sql.TxOptions{
    Isolation: sql.LevelRepeatableRead,
}, func(tx *orm.Transaction) error {
    users, err := repo.InTransaction(tx)
    if err != nil {
        return err
    }
    return users.PatchByID(tx.Context(), id, map[string]any{"active": false})
})
```

One callback covers one database pool. Nested managed transactions are rejected;
there is no cross-pool transaction guarantee or automatic callback retry. Bulk
GORM operations may use internal savepoints. Repository calls inside the callback
must use tx.Context or a derived context; transaction repositories expire as soon
as the callback returns. Passing a transaction context to a root repository is
rejected. Callbacks must use the transaction repository for every participating
write, execute sequentially and propagate errors. Native GORM escape hatches
cannot enforce these programming guards. Panic rolls back and is rethrown.

InTransaction retains the root repository's model, tenant Scope, column allowlists,
version policy and resource limits. A different Client (even another pool pointing
at the same database) is rejected with ErrTransactionClientMismatch. Bind root
repositories only; expired or already transaction-bound repositories are rejected.
NewTransactionRepository is a low-level constructor for a new policy: it cannot
infer an existing repository's tenant/client policy and must not replace binding
when preserving that policy matters.

All WithTransaction errors expose *TransactionError through errors.As. Phase is
admission, begin, setup, callback or commit; Outcome is not_started, rolled_back,
committed or unknown. Underlying error chains remain available through errors.Is/As;
RollbackError() reports a cleanup failure separately. Every failed commit is
conservatively classified unknown and matches ErrCommitUnknown, even if the driver
could in some cases prove rollback. A lost commit acknowledgement may occur after
data was persisted. No automatic retry is performed. A panic attempts rollback,
records its outcome and is rethrown; transaction callbacks must remain cooperative.

Managed PostgreSQL transactions also apply transaction-local statement_timeout,
lock_timeout and idle_in_transaction_session_timeout. Defaults are 10 seconds,
1 second and 30 seconds respectively, independently of client deadlines. Configure
postgres.New(dsn).WithTimeouts(postgres.Timeouts{...}) explicitly for your deployment. Zero inherits the role/database
setting; it does not disable an existing server limit. Nonzero settings only
shorten inherited limits: a stricter existing server limit is preserved. Values must be at
least 1ms and at most 24h. Settings use bound values and set_config(..., true), and
do not survive commit/rollback into the pooled session. Other dialects ignore this
PostgreSQL policy. Ordinary/native queries and implicit GORM write transactions do
not receive these local settings; configure role/database limits for those paths.

Use `errors.Is` for sentinel errors and cancellation. Database errors have a safe
`*orm.Error` with Operation, Kind and validated SQLState. PostgreSQL, MySQL and SQLite constraint errors map to ErrConflict through their
adapters. Error also exposes a sanitized DriverCode; SQLState is set when the
driver supplies it. Deadlock, serialization, lock timeout and statement timeout
classes have separate sentinels where the adapter can identify them. `errors.As`
preserves native driver errors for structured handling; do not log their raw
message/detail, which may contain row values or connection information. Commit
failure can have an unknown outcome. Reconcile with application idempotency keys
or an outbox before retrying external side effects.

## Privacy and observability

The built-in slog adapter records duration, affected rows and a safe error class.
It never emits SQL, parameter values, identifiers or raw driver error text, even
for slow/failed queries and literal SQL. GORM Debug cannot enable payload logging
through this adapter. OTel traces use constant operation names and row counts;
query text, exception text, DSNs and table names are excluded. Inject Logger and
TracerProvider and MeterProvider per client. The application owns exporter
shutdown. Transaction spans record fixed phase/outcome metadata, never exception
messages. Stats(), AdmissionStats() and TransactionStats() expose pool wait,
admission pressure and transaction outcomes/duration. Optional synchronous Monitor
events provide fixed metadata without raw error payloads; callbacks must be fast,
concurrency-safe and must not invoke lifecycle operations. Callback panics are isolated.

With EnableTelemetry, OTel exports orm.admission.wait (seconds),
orm.admission.rejected, orm.transaction.duration (seconds) and orm.transactions,
plus observable orm.pool.connections, orm.pool.waits and orm.pool.wait_time
(seconds). Admission wait is distinct from SQL pool wait. Pools carry a generated,
non-sensitive orm.client.id to keep different clients' observations separate.
The per-pool callback is unregistered when the client drains and closes; no
reporting goroutines are created. Native GORM returned errors are raw and must
be sanitized by callers.

## Migrations and verification

Open never runs AutoMigrate. Deploy versioned SQL through a separate migration
job; use an expand/backfill/contract rollout and plan rollback before destructive
schema changes. `db/migration` currently wraps Goose global configuration, so run
it in a dedicated process rather than configuring different dialects concurrently.
The runnable SQLite example uses AutoMigrate only for its disposable schema.

From the repository root:

```text
go test ./...
go vet ./...
go test -race -count=10 ./db/orm/...
go test ./db/orm -run '^$' -fuzz FuzzPageValidation -fuzztime 10s
go test ./db/orm -run '^$' -fuzz FuzzCursorDecode -fuzztime 10s
go test ./db/orm -run '^$' -fuzz FuzzBoundID -fuzztime 10s
```

For integration, supply ORM_POSTGRES_DSN for a disposable PostgreSQL database and
set ORM_INTEGRATION_REQUIRED=1 so unavailable fixtures cannot silently skip:

```text
go test -race -tags=integration -count=1 ./db/orm/...
go test -tags=integration -run '^$' -bench BenchmarkPostgresCursor -benchtime=2s -benchmem ./db/orm
```

Integration tests create/drop a unique schema. The dedicated CI workflow covers
PostgreSQL 16/17/18, MySQL 8.4, race tests, fuzz smoke tests and a bounded-pool
benchmark. SQLite always runs the same core contract in unit tests. Supply
ORM_MYSQL_DSN and set ORM_MYSQL_REQUIRED=1 for required MySQL integration; the
fixture account needs create/drop table and metadata access in a disposable DB.
MySQL uses unique table prefixes, PostgreSQL uses unique schemas. Never run
these fixtures against an application database.
The workload asserts pool capacity and return of borrowed connections; benchmark
numbers are baselines, not production latency promises. Before rollout, benchmark
with your schema, indexes, replica count and traffic, and define latency/error
budgets plus alerts on connection wait time. TLS-specific deployment behavior and
database failover need environment-specific staging checks.

## Breaking replacement

This module had no published consumer contract; obsolete APIs were removed:

| Previous API | Replacement |
| --- | --- |
| New(Config) | Open(ctx, Config) returning an owning Client |
| Config.Dialector / Config.Postgres | Config.Backend with explicit adapter options |
| Global Init/Get/AddConnection/Close | Instance Manager.Init/Lookup/Add/Close |
| NewRepository(gormDB) | NewRepository(client, explicit options), returning error |
| WithTx | rootRepository.InTransaction inside WithTransaction |
| Insert / InsertMany | Create / CreateMany |
| Update / UpdateColumns | PatchByID with explicit scalar map |
| Delete / Restore | DeleteByID / RestoreByID |
| Raw string Sorts / ExecutePagination | Typed Sorts and Repository.Paginate |
| Implicit UpdateAll upserts | UpsertMany with explicit UpsertPolicy |
| Unbounded/raw/aggregate repository helpers | Typed application queries via native DB |

There is no silent compatibility shim. Update imports/call sites against the new
contracts before introducing this module to services.

For database writes that must eventually publish an event, see the
[outbox and inbox recipe](../OUTBOX.md). A broker publication does not join an ORM
transaction. Use stable business keys to reconcile unknown commit outcomes.

# Database packages

The `db` directory provides SQL access through GORM, MongoDB access through the
MongoDB Go driver, and versioned SQL migrations through Goose. These packages are
part of the single `github.com/thanhbvha/go-common` module and require Go 1.26.0.
They are independent of HTTP frameworks and use explicit client/manager ownership.

## Choose a package

| Package | Purpose | Documentation and examples |
| --- | --- | --- |
| `db/orm` | SQL pools, bounded generic repositories, managed transactions, and observability | [API guide](orm/README.md) · [14 runnable demos](../examples/db/README.md) |
| `db/mongodb` | MongoDB clients, named database bindings, repositories, and managed transactions | [Production APIs](mongodb/README.md) · [Runnable examples](../examples/db_mongodb/README.md) |
| `db/migration` | Versioned SQL migrations using Goose, with filesystem or embedded SQL files | [Package documentation](migration/doc.go) · [Implementation and options](migration/migration.go) |

## SQL backends

Select an adapter explicitly and assign it to `orm.Config.Backend`:

| Adapter import suffix | Supported profile | Important differences |
| --- | --- | --- |
| `db/orm/backend/postgres` | PostgreSQL 16–18 | Strict upsert targets, read-only transactions, and transaction-local server timeouts |
| `db/orm/backend/mysql` | Oracle MySQL 8.4 LTS, InnoDB, strict SQL mode | Upsert requires explicit `AnyUniqueKey`; strict targets and `DoNothing` are unsupported |
| `db/orm/backend/sqlite` | Bundled pure-Go driver, SQLite 3.35+ | No read-only transaction mode or transaction-local server timeouts; plan for limited write concurrency |

Other engines need a dedicated backend adapter and contract verification. SQL syntax,
migrations, isolation levels, and upsert behavior are not universally portable.
`client.Capabilities()` reports the verified runtime profile; `Config.Require`
can reject a backend that lacks features required by the application.

## Main capabilities

**SQL ORM** provides instance-owned pools and named multi-pool managers, startup
validation, finite connection/time/resource budgets, bounded admission, and graceful
draining. Generic repositories support CRUD, zero-value patches, soft delete/restore,
tenant scopes, bulk writes, capability-aware upserts, offset/cursor pagination, and
optimistic concurrency with compare-and-swap. Managed transactions preserve repository
policies and reject invalid context, expired transaction, and wrong-client usage.
Typed errors, pool/admission/transaction statistics, and optional OpenTelemetry
providers support operational visibility.

**MongoDB** provides instance-scoped managers, explicit client sharing through
`ClientGroup`, CRUD and bulk operations, indexes, bounded reads, streaming callbacks,
offset/cursor pagination, capability checks, managed transaction repositories, and
transaction draining/monitoring. Transaction support depends on deployment topology;
set `RequireTransactions` when it is a startup requirement. See the MongoDB guides
for driver retry semantics and the differences between direct and managed operations.

**Migrations** provide `Up`, `Down`, `Status`, `UpTo`, and `DownTo`, and accept an
`embed.FS` so SQL files can ship inside the application binary. Migrations are
explicit; opening an ORM pool does not create or migrate application tables.

## SQL quick start

This PostgreSQL example assumes versioned migrations have already created
`patients` with an auto-generated primary key `id` and a `name` column. Supply
`DATABASE_DSN` through deployment configuration, including the required TLS/auth
settings. The function returns shutdown errors as well as operation errors.

```go
package dbexample

import (
    "context"
    "errors"
    "os"
    "time"

    "github.com/thanhbvha/go-common/db/orm"
    "github.com/thanhbvha/go-common/db/orm/backend/postgres"
)

type Patient struct {
    ID   uint `gorm:"primaryKey"`
    Name string
}

func CreateSQLPatient(ctx context.Context) (result error) {
    dsn := os.Getenv("DATABASE_DSN")
    if dsn == "" {
        return errors.New("DATABASE_DSN is required")
    }
    cfg := orm.DefaultConfig()
    cfg.Backend = postgres.New(dsn)
    client, err := orm.Open(ctx, cfg)
    if err != nil {
        return err
    }
    defer func() {
        shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        result = errors.Join(result, client.Close(shutdownCtx))
    }()

    repo, err := orm.NewRepository[Patient](client, orm.RepositoryOptions{
        WritableColumns: []string{"name"},
    })
    if err != nil {
        return err
    }
    return repo.Create(ctx, &Patient{Name: "Example"})
}
```

Use `orm.NewManager()` with `Init`, `Lookup`, and `Add` for named pools. Bind an
existing repository with `repo.InTransaction(tx)` inside `client.WithTransaction`
to retain its policy, and pass `tx.Context()` or a child context to its operations.
The [ORM guide](orm/README.md) covers configuration, transaction errors, and backend
contracts; the [SQL examples](../examples/db/README.md) exercise each feature.

To explore without a database server, run from the repository root:

```sh
go run ./examples/db -list
go run ./examples/db -demo all -otel
```

The demos use in-memory SQLite by default. Network backends require environment
DSNs and a dedicated test database; the runner creates and drops scratch tables.

## MongoDB quick start

Supply `MONGODB_URI` through deployment configuration. This CRUD example uses the
`example` database and does not require transaction support. Configure indexes and
collection validation separately for the application's schema.

```go
package dbexample

import (
    "context"
    "errors"
    "os"
    "time"

    "github.com/thanhbvha/go-common/db/mongodb"
)

type MongoPatient struct {
    Name string `bson:"name"`
}

func CreateMongoPatient(ctx context.Context) (result error) {
    uri := os.Getenv("MONGODB_URI")
    if uri == "" {
        return errors.New("MONGODB_URI is required")
    }
    cfg := mongodb.DefaultConfig()
    cfg.URI = uri
    cfg.DBName = "example"
    client, err := mongodb.New(ctx, cfg)
    if err != nil {
        return err
    }
    defer func() {
        shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        result = errors.Join(result, client.Disconnect(shutdownCtx))
    }()

    repo := mongodb.NewRepository[MongoPatient](client.Database(cfg.DBName), "patients")
    _, err = repo.InsertOne(ctx, &MongoPatient{Name: "Example"})
    return err
}
```

For multiple database bindings, client groups, bounded reads, transactions, and
shutdown behavior, see the [MongoDB API guide](mongodb/README.md) and
[MongoDB examples](../examples/db_mongodb/README.md). Raw driver errors may contain
sensitive data; sanitize them before logging.

## SQL migrations

Keep migration SQL specific to the chosen backend. The example below expects Goose
migration files under `sql/` next to the Go source containing the embed directive.
The directory must contain at least one matching file when compiling.

```go
package dbexample

import (
    "embed"

    "github.com/thanhbvha/go-common/db/migration"
    "github.com/thanhbvha/go-common/db/orm"
)

//go:embed sql/*.sql
var migrationFS embed.FS

func MigratePostgres(client *orm.Client) error {
    pool, err := client.DB().DB()
    if err != nil {
        return err
    }
    migrator, err := migration.New(migration.Options{
        DB:      pool,
        Dialect: "postgres", // Use "mysql" or "sqlite3" for those backends.
        Dir:     "sql",
        FS:      migrationFS,
    })
    if err != nil {
        return err
    }
    return migrator.Up()
}
```

The extracted `*sql.DB` remains owned by the ORM client; do not close it separately.
Run migrations as a controlled deployment step before serving traffic, outside
managed repository transactions. MySQL DDL can implicitly commit. Goose migration
support does not imply ORM adapter support for every Goose dialect.

The current migration wrapper configures Goose's package-global dialect and base
filesystem. Avoid concurrent migration managers with different dialects/filesystems
in one process; use a dedicated migration process or serialize a consistent setup.
Its methods do not accept contexts, so ORM query/admission timeouts do not bound
migration execution. Do not log raw migration errors without sanitizing them.

## Operational and security boundaries

- Start from `DefaultConfig()` and tune aggregate pool capacity across all service
  replicas. Give operations deadlines, keep transaction callbacks short, and stop
  accepting traffic before draining clients. SQL `Close` is terminal; if draining
  times out, retry with a fresh context after active callbacks return.
- A transaction is not atomic across independent SQL pools, SQL and MongoDB, or
  unrelated MongoDB clients. Use outbox/idempotency/saga patterns where required.
  MongoDB transaction topology and driver retry rules differ from SQL semantics.
- Parameterize values and allowlist dynamic identifiers. Repository scopes and
  callbacks are trusted application code; derive tenant identity from authenticated
  context and validate ownership on inserts. Native GORM/driver access bypasses
  managed repository policies and limits.
- SQL `ErrCommitUnknown` means data may already have committed. Reconcile using
  business identifiers before retrying; do not blindly retry writes. Consult each
  package's error and transaction contract rather than assuming identical behavior.
- Keep credentials in deployment configuration, preserve TLS verification, and never
  log DSNs, document contents, or raw driver error payloads.

## Observability and further reading

Instrumentation is opt-in through `EnableTelemetry`; exporting telemetry requires
application-configured OpenTelemetry SDK providers/exporters. SQL accepts per-client
`TracerProvider` and `MeterProvider`; MongoDB command tracing uses the application's
OpenTelemetry tracer provider. These wrappers do not install exporters or collectors.
SQL traces/logs exclude SQL text, parameter values, and DSNs; MongoDB command traces
exclude query document contents. Application logging and native APIs need the same
care independently.

Use SQL `Stats`, `AdmissionStats`, and `TransactionStats` for pool/queue/transaction
visibility. MongoDB provides driver pool hooks and managed transaction statistics
and monitoring. Observer callbacks must be fast, concurrency-safe, and must not
reenter lifecycle methods. Close database clients before shutting down their
application-owned telemetry providers.

- [SQL API, backend contracts, and production guidance](orm/README.md)
- [SQL demo commands and expected results](../examples/db/README.md)
- [MongoDB production APIs](mongodb/README.md)
- [MongoDB configuration and runnable examples](../examples/db_mongodb/README.md)

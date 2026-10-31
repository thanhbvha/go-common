# MongoDB

`db/mongodb` wraps the official Go driver v2. Prefer an instance manager for each
service/test. Existing package-level `Init`, `Get`, `AddConnection`,
`WithTransaction`, and `DisconnectAll` remain available through a shared manager.

## Run the complete example

Run commands from the repository root. Use an isolated replica set, such as the
Docker setup in **Live MongoDB integration tests** below. The example creates
three randomly named `go_common_example_<ObjectID>_*` databases, writes synthetic
fixtures, checks the results, then drops only those generated databases and closes
clients. It never uses a database name supplied through the connection URI.
It needs permission to create/drop the temporary databases and create indexes;
do not run it against production. Forced termination can prevent cleanup.

```powershell
$env:MONGODB_URI = "mongodb://127.0.0.1:27028/?directConnection=true&replicaSet=go_common_it"
go run ./examples/db_mongodb -example all
```

Run the package directory, not `main.go` alone: the scenarios are implemented in
multiple Go files in this package.

The default is `all`. Select one scenario with `-example <name>`:

| Name | Demonstration |
| --- | --- |
| `connections` | `Init` for two aliases, `AddConnection` for audit, default/named lookup; three databases sharing one client, plus an independent client on the same URI |
| `capabilities` | Manager/native deployment detection, sessions, transaction support and wire version; startup requires transactions |
| `diagnostics` | Typed configuration/group errors with `errors.Is`/`errors.As`, safe field/code diagnostics and rejected alias verification |
| `crud` | Insert/find/update/delete, `ErrNotFound`, unique indexes, upsert, bulk writes, distinct and aggregation |
| `options` | Typed inserts, array filters in a transaction, unordered partial write results, index hints and forced upsert semantics |
| `pagination` | Exact/count-free pages; indexed cursor pages in both directions, projection, malformed tokens and direction mismatch |
| `stream` | `FindEach`, `AggregateEach`, typed aggregate output, bounded batches, callback errors and cancellation |
| `watch` | Change Streams: notification inserts, update filtering, typed callbacks, in-memory resume and cancellation |
| `transaction` | Commit order/stock/outbox across three databases, idempotent replay, rollback on insufficient stock, single-alias API |
| `retry` | Two real writes followed by one synthetic transient callback error; verify rollback and driver retry without duplicate records |
| `observability` | Concurrency-safe transaction hook, duration, statistics and verified commit/rollback/retry outcomes |
| `guards` | Group conflict, different client, nested transaction, undeclared alias, wrong context/deadline and use after callback |
| `lifecycle` | Repeated `Init`, shutdown, closed-handle lookup and reinitialization with a fresh client |
| `drain` | Cancellable lifecycle wait, shutdown timeout retaining clients, rejection of new work, active commit and shutdown retry |
| `global` | Global `Init`, `Lookup`, `Get`, capabilities/statistics, legacy/scoped transactions with options, `DisconnectAll` |

For example:

```powershell
go run ./examples/db_mongodb -example transaction
go run ./examples/db_mongodb -example retry
go run ./examples/db_mongodb -example guards
go run ./examples/db_mongodb -example capabilities
go run ./examples/db_mongodb -example diagnostics
go run ./examples/db_mongodb -example options
go run ./examples/db_mongodb -example observability
go run ./examples/db_mongodb -example drain
go run ./examples/db_mongodb -example watch
```

Each selection creates its own fixtures and cleans up independently. Every scenario
prints `<name>: OK` only after verifying its expected outcome; errors exit nonzero.
The final line confirms cleanup. Raw database errors, document contents and URIs
are omitted from output. The overall run has a 60s context; shutdown uses a fresh,
bounded context even when the run fails.

Code navigation:

- [main.go](main.go): argument selection, instance wiring, client ownership, shutdown.
- [repository.go](repository.go): models, fixtures, indexes, CRUD, pagination, streaming.
- [capabilities.go](capabilities.go): deployment capabilities and typed diagnostics.
- [options.go](options.go): driver options, partial write results and transaction guards.
- [observability.go](observability.go): bounded atomic counters, duration and outcome checks.
- [drain.go](drain.go): coordinated transaction/shutdown with cancellation and retry.
- [watch.go](watch.go): notification change events and an application-owned sender callback.
- [transactions.go](transactions.go): conditional stock update, stable request ID,
  order/outbox atomicity, commit/replay/rollback and the legacy session API.
- [retry.go](retry.go): explicit demo-only transient error; no server failpoint or
  admin command is required. Production code must not fabricate error labels.
- [guards.go](guards.go): expected error handling with `errors.Is`.
- [global.go](global.go): deliberately shared process-global API compatibility.
- [lifecycle.go](lifecycle.go): independent owner, repeated initialization and restart.

## What the new scenarios verify

`diagnostics` reports only configuration fields/codes and conflicting field names.
It checks that invalid configurations never publish a connection alias.
`options` demonstrates checking partial results alongside duplicate-key errors from
unordered inserts/bulk writes, using array filters inside a scoped transaction,
rejecting a wrong transaction context, and preserving upsert semantics.

`observability` runs three transactions: one commit, one business rollback and one
callback retry followed by commit. It checks statistics deltas of started=3,
committed=2, failed=1, attempts=4, retries=1 and active=0 against persisted results.
The hook records bounded, concurrency-safe atomic counters and duration without
logging document data or retaining event history. Hooks must be fast, concurrency-safe
and must not call lifecycle methods. Statistics belong to each manager; the example
shares one hook pointer across grouped configurations. An unknown commit result is
a separate outcome; fault injection for that path belongs to the integration suite.

`drain` holds a transaction open on a separate manager, cancels a lifecycle wait,
then lets shutdown time out. It verifies that clients remain owned, new transactions
and lookups are rejected, and the existing transaction can still commit exactly once.
A final shutdown retry closes that manager. The 30ms deadlines make the demonstration
short; choose shutdown budgets appropriate to your application in production.

The order function assumes authenticated, authorized service input. It validates
positive quantity, uses a unique request ID, rejects reuse with different intent,
and decrements only when stock is sufficient. The outbox record is committed with
the order; an outbox publisher/consumer is not implemented in this MongoDB example.
Callbacks contain no HTTP calls or external publication. Publishing later still
requires idempotent delivery handling. This is an instructional service operation,
not a complete checkout API, tenancy layer or production inventory system.

```go
manager := mongodb.NewManager()
cfg := mongodb.Config{
    URI: os.Getenv("MONGODB_URI"),
    DBName: "app",
    AppName: "patient-api",
    EnableTelemetry: true,
}
if err := manager.Init(ctx, map[string]mongodb.Config{"main": cfg}, "main"); err != nil {
    return err
}
defer func() {
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    if err := manager.DisconnectAll(shutdownCtx); err != nil {
        // Report a sanitized shutdown error through application logging.
    }
}()
db, err := manager.Lookup("") // empty name selects the default; returns errors
if err != nil {
    return err
}
repo := mongodb.NewRepository[Sample](db, "samples")
```

## Configuration and compatibility

- Use a deployment-provided URI with authentication and verified TLS. Never log
  credentials or pass arbitrary user input as a connection URI.
- URI is required. Managed connections also require a nonempty valid `DBName`
  and connection name. All configurations/default names are checked before dialing.
- Nonzero `MaxPoolSize`, `MinPoolSize`, `ConnectTimeout`, and `OperationTimeout`
  override URI settings. Zero preserves URI settings. Without a URI setting,
  defaults are max pool 100, min pool 0, connect timeout 10s, operation timeout 30s.
- `PingTimeout` defaults to 5s when zero. Negative timeout fields are rejected.
  Explicit URI values such as `maxPoolSize=0`, `connectTimeoutMS=0`, or `timeoutMS=0`
  retain driver semantics (unlimited/disabled). Use them deliberately.
- `DefaultConfig()` is a local development convenience. Its nonzero fields override
  matching URI values; use `Config{URI: ..., DBName: ...}` to preserve URI options.
- `OperationTimeout` is a driver client timeout, not a wall-clock budget for an
  entire pagination/export workflow. Give each request/workflow a context deadline;
  exports may need a longer one. Driver timeout behavior still applies to operations
  inside that workflow.
- Telemetry excludes command text and document values. `PoolMonitor` can receive
  driver pool events for metrics; its callbacks must be fast and concurrency-safe.
- `Init` is atomic for newly opened connections. Failures roll back those clients
  and preserve connections previously added with `AddConnection`. Repeated successful
  `Init` calls are no-ops, not configuration merges. Use `AddConnection` explicitly.
- One configured connection becomes the default automatically. With multiple
  connections, specify a default or always use named lookup. Explicit defaults win.
- `DisconnectAll` rejects new lookups/transactions and drains manager-owned
  transactions before closing clients. If draining times out, clients stay owned
  and closing; retry shutdown after active work ends. Stop raw client operations
  yourself and supply a fresh, bounded shutdown context.
- Global `Get` retains panic-on-misconfiguration behavior. Prefer `Lookup` for
  recoverable errors. Managers must not be copied after first use.

Behavior changes: zero timeout fields no longer cause immediate ping expiration;
zero pool fields no longer silently override the URI; default min pool is now 0
(formerly 10); unspecified operations now have a 30s fallback timeout; query contents
are excluded from traces; pagination owns skip/limit and defaults to `_id: 1` sorting.

## Multiple databases and transaction scopes

Set `ClientGroup` explicitly to bind N database aliases to one client. An empty
group preserves the existing private-client-per-alias behavior, even with identical
URIs. Private alias names and shared group names have independent namespaces.

```go
base := mongodb.Config{
    URI: os.Getenv("MONGODB_URI"),
    DBName: "orders",
    ClientGroup: "business",
    AppName: "order-service",
    TransactionTimeout: 10*time.Second,
}
inventory := base
inventory.DBName = "inventory"
if err := manager.Init(ctx, map[string]mongodb.Config{
    "orders": base,
    "inventory": inventory,
}, "orders"); err != nil {
    return err
}

// This API is also available as mongodb.WithTransactionAcross for the global manager.
err := manager.WithTransactionAcross(ctx, []string{"orders", "inventory"}, func(tx *mongodb.Transaction) error {
    orders, err := mongodb.NewTransactionRepository[Order](tx, "orders", "orders")
    if err != nil {
        return err
    }
    stock, err := mongodb.NewTransactionRepository[Stock](tx, "inventory", "stock")
    if err != nil {
        return err
    }
    if _, err := orders.Count(tx.Context(), bson.M{}); err != nil {
        return err
    }
    _, err = stock.FindByID(tx.Context(), "demo-sku")
    return err
}, options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()))
```

This snippet illustrates session wiring. The runnable order/stock/outbox write
flow, including quantity and matched-count checks, is in [transactions.go](transactions.go).

- Group configurations must match **exactly**, except `DBName` (and the group
  identifier itself). Clone one base config and change `DBName`; a zero-valued
  literal and a `DefaultConfig()` may be semantically similar but are not equal
  configurations. URI, credentials, TLS, pool settings, monitor pointer, telemetry,
  application name and timeout policies cannot silently differ within a group.
- `Init` validates group conflicts before dialing. Startup failure rolls back only
  newly created clients, once per client, and preserves previously added bindings.
  `AddConnection` joins an existing group only with matching configuration.
  `DisconnectAll` closes shared clients once and clears client/binding state.
- Scope creation checks every alias and rejects different clients before invoking
  the callback (`ErrTransactionClientMismatch`), including separate clients aimed
  at the same cluster. Deployment-independent transactions require a Saga/outbox.
- `TransactionTimeout` supplies a whole-transaction deadline when the incoming
  context has none; zero defaults to 30s, negative values fail validation. An existing
  context deadline takes precedence. Cancellation is cooperative; callbacks must
  observe the context. Driver transaction/commit retries remain authoritative.
- Both scope APIs accept driver transaction options. Existing global
  `WithTransaction` retains its original signature; the instance method retains
  its options parameter. Both reject a context containing an existing MongoDB
  session (`ErrNestedTransaction`), including a session not currently in a transaction.
  This intentionally prevents silently replacing outer sessions or nested commits.
- A transaction repository requires `tx.Context()` or a derived context preserving
  its deadline/session. Wrong contexts fail with `ErrTransactionContext` before DB
  access. Every retry creates a new scope; retained repositories fail with
  `ErrTransactionClosed` after an attempt ends. Propagate these errors to abort.
- `tx.Database(alias)` permits only declared aliases (`ErrTransactionScope`). It
  returns a native handle: native driver operations and `Collection()` bypass the
  repository guards. Ordinary repositories also retain existing context behavior.
  Scope guards are not authorization or a mechanism to prevent deliberate misuse.
- Use sessions sequentially. Concurrent transactions may each have their own scope,
  but goroutines must not operate concurrently inside one scope. Shutdown drains
  transactions started through the manager; direct client/session operations are
  outside that tracking.

## Reads and pagination

```go
// Existing exact-count response: TotalRows, TotalPages, Page, Size, Items.
page, err := repo.Paginate(ctx, bson.M{"group": "fixture"}, mongodb.PageRequest{
    Page: 1, Size: 50,
}, options.Find().SetSort(bson.D{
    {Key: "score", Value: -1}, {Key: "_id", Value: -1},
}).SetProjection(bson.M{"_id": 1, "score": 1}))

// One query, fetching size+1 documents; no CountDocuments round trip.
slice, err := repo.PaginateSlice(ctx, bson.M{"group": "fixture"}, mongodb.PageRequest{
    Page: 1, Size: 50,
}) // Page, Size, HasMore, Items

// Streaming application processing, with bounded driver batches.
err = repo.FindEach(ctx, bson.M{"group": "fixture"}, func(ctx context.Context, sample *Sample) error {
    // Process one synthetic document here; propagate any processing error.
    return nil
}, options.Find().SetBatchSize(100))
```

Page defaults to 1, size defaults to 10 and is capped at 1000. Overflowing offsets
fail before database access. Caller skip/limit options cannot bypass those bounds.
`Sorts` is retained for compatibility but is not parsed; allowlist sort fields in
business code and pass `SetSort` with ordered `bson.D`. Custom sorts should include
a unique tie-breaker. Count and items are separate reads, not one snapshot.

`FindAll` and `Aggregate` materialize all results; use `FindEach` and `AggregateEach`
for large result sets. `ExecuteAggregateEach[Output]` supports a different output
type, such as a summary. Streaming stops on callback/decode/cursor error and closes
the cursor even after cancellation. For deep pagination, `PaginateCursor` uses an
indexed `_id` range, requires a uniform scalar `_id` type and retains `_id` in the
projection. Treat its token as opaque and preserve the direction between requests.
Custom cursor fields need application-defined range queries through `Collection()`.
Offset pagination remains expensive at high offsets, including `PaginateSlice`.

## Watching notifications

`watch` demonstrates the MongoDB equivalent of Node.js `collection.watch()`.
It opens a stream on `portal_users_notification` in this run's generated temporary
database before inserting fixtures. It verifies two insert payloads in order and
filters an update between them. It then closes that stream, inserts a third fixture,
and uses `WatchEach` with the last in-memory resume token to receive it. ObjectID
is converted with `Hex()` for the application payload. The sender is a bounded
in-memory fake; nothing is sent to Socket.IO or another external service.

`WatchEach` is a blocking worker. An application can run it like this, using the
`Notification`, `notificationSender` and `sendNotification` types from `watch.go`:

```go
workerCtx, cancelWorker := context.WithCancel(serviceCtx)
done := make(chan error, 1)
go func() {
    done <- repo.WatchEach(workerCtx, mongo.Pipeline{
        bson.D{{Key: "$match", Value: bson.M{"operationType": "insert"}}},
    }, func(ctx context.Context, change *mongodb.ChangeEvent[Notification]) error {
        return sendNotification(ctx, sender, change)
    }, options.ChangeStream().SetBatchSize(100).SetMaxAwaitTime(time.Second))
}()

// When stopping the service, cancel/join workers before closing their manager.
cancelWorker()
if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
    // Report sanitized worker failure; do not print document/connection data.
}
shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
defer cancelShutdown()
err := manager.DisconnectAll(shutdownCtx)
```

Supervise `done` during normal operation as well, so a worker failure is visible
immediately. Each callback must return and observe its context. Keep worker
lifetimes separate from HTTP request deadlines, and keep `MaxAwaitTime` below the
configured operation timeout. The native `Watch` API supports opening the stream
synchronously when startup readiness must be coordinated with writers.

Change Streams require a replica set or supported sharded deployment and the
appropriate database privileges. They cannot run in a transaction. `FullDocument`
is optional on the general event type; the example filters inserts, which include
it. For update document lookup, explicitly supply `options.UpdateLookup`.

This version does not persist checkpoints or retry sender failures. Driver resume
handles eligible errors while the stream is running; terminal errors return to
the application. Restarting without a resume position can miss downtime events.
Multiple watchers each receive the changes; they do not share a work queue. The
manager's transaction drain does not wait for these workers. Real delivery,
idempotency and room authorization belong to the application/transport layer.

## Production operation

- Reuse clients across requests. With no `ClientGroup`, each configuration opens
  a separate client; clients are not deduplicated by URI. Databases sharing credentials
  and client policies can share a `ClientGroup`, or use `db.Client().Database("audit")`.
  The manager/client owner handles shutdown.
- Build indexes from real query filter/sort patterns and inspect
  `explain("executionStats")`. Apply index changes through a controlled migration;
  do not create indexes per request. Use unique indexes for business uniqueness.
- Authorize tenant/account filters in domain repositories. Generic CRUD does not
  provide authorization. Do not forward raw request JSON as BSON filters/updates.
- Check matched/deleted counts explicitly. A successful Update/Delete may affect
  zero documents; the wrapper preserves driver semantics. Other driver options
  remain available through `Collection()`.
- Transactions require a replica set or supported sharded deployment. Instance
  `WithTransaction` accepts transaction options; set read/write concerns according
  to durability requirements. Use callback session context for every operation.
  Callbacks may repeat: propagate errors and use idempotency/outbox for external
  effects. Do not share a session across concurrent operations.
- Monitor latency/error rate, pool checkout waits/failures and total connections.
  Size pools using service replicas and per-server client pools, then load test.
  Configure readiness, failover, backups and restore drills in the deployment.

## Verification

```sh
go test ./db/mongodb ./examples/db_mongodb
go test -race -count=10 ./db/mongodb
go test ./...
go vet ./...
```

Unit tests use the driver's wire-protocol mock; they do not establish live broker,
replica-set failover, transaction durability or production throughput guarantees.
Run deployment-specific integration and load tests before release.

### Live MongoDB integration tests

The opt-in suite in `db/mongodb/mongodb_integration_test.go` requires an isolated
replica set. Each run generates fresh `go_common_it_<ObjectID>` databases, writes
only synthetic fixtures, and drops those databases before disconnecting. Never use
a production URI. No credentials are embedded in the tests.

For a disposable single-node replica set, run the following in PowerShell from
the repository root (requires Docker and a free localhost port 27028):

```powershell
docker run --rm --detach --name go-common-mongodb-it --publish 127.0.0.1:27028:27017 mongo:8.0.16 --replSet go_common_it --bind_ip_all --setParameter enableTestCommands=1
$mongoReady = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    docker exec go-common-mongodb-it mongosh --quiet --eval 'quit(db.adminCommand({ping:1}).ok === 1 ? 0 : 1)' *> $null
    if ($LASTEXITCODE -eq 0) { $mongoReady = $true; break }
    Start-Sleep -Milliseconds 500
}
if (!$mongoReady) { throw "Temporary MongoDB did not become ready" }
docker exec go-common-mongodb-it mongosh --quiet --eval 'rs.initiate({_id:"go_common_it",members:[{_id:0,host:"localhost:27017"}]}); for(let i=0;i<60;i++){if(db.hello().isWritablePrimary){quit(0)}sleep(500)}quit(1)'
$env:MONGODB_INTEGRATION_URI = "mongodb://127.0.0.1:27028/?directConnection=true&replicaSet=go_common_it"
$env:MONGODB_TEST_FAILPOINTS = "1"
go test -race -tags=integration -run '^TestMongoDBIntegration$' -count=1 -v -timeout=120s ./db/mongodb
go test -race -tags=integration -run '^TestMongoDBExampleAll$' -count=1 -v -timeout=120s ./examples/db_mongodb
$env:MONGODB_URI = $env:MONGODB_INTEGRATION_URI
go run ./examples/db_mongodb -example all
docker stop go-common-mongodb-it
Remove-Item Env:MONGODB_INTEGRATION_URI, Env:MONGODB_TEST_FAILPOINTS, Env:MONGODB_URI
```

Stop only the disposable container created successfully by the first command;
the `--rm` flag removes it on stop, and no host data volume is mounted.
The localhost-only, test-command-enabled server above is for testing only.
`MONGODB_TEST_FAILPOINTS=1` enables server-wide fault injection filtered to the
suite's application name. Use it only on a dedicated test server; each fault is
applied once and reset during cleanup. Omit it when using a test server without
`enableTestCommands=1` or without admin privileges.

Coverage includes CRUD/ObjectID/not-found contracts, unique indexes, bulk writes,
aggregation, exact-count and count-free pagination bounds, multi-batch streaming,
cancellation, transaction commit/rollback/retry, manager rollback/reinitialization,
cross-database atomicity and scope guards, concurrent reads/writes, trace data
exclusion, and deadline/pool recovery.
The disposable setup above has one node. The dedicated CI fixtures below also
exercise multi-node failover and TLS/authentication; neither measures production
throughput or proves backup/restore guarantees.

### Production-oriented APIs and CI

See [module production guide](../../db/mongodb/README.md) for capabilities,
context-aware lifecycle/draining, transaction statistics/hooks, cursor pagination,
streaming aggregation, typed configuration errors and `WithOptions` repository APIs.
All 15 runnable scenarios above verify the public APIs. Startup uses
`RequireTransactions: true`, so a
standalone server fails early with a clear message instead of failing mid-demo.

The MongoDB workflow runs two disposable fixtures using the same script:

```bash
bash .github/scripts/test-mongodb.sh replica
bash .github/scripts/test-mongodb.sh secure
```

Use Git Bash on Windows. `replica` binds localhost ports 27031–27034 and runs three
independent replica-set processes plus a standalone for capability rejection.
`secure` binds localhost port 27041, creates an ephemeral CA/server certificate
and SCRAM credentials, and requires verified TLS/authentication. All databases,
containers, certificates and credentials are test-only and cleaned up by the script.
It does not use or change the standalone MongoDB on port 27017.

CI runs unit race checks and tagged integration tests for module and runnable examples.
Fault tests cover primary step-down, unknown commit response without callback replay,
draining before commit, cursor pagination, array filters, unordered writes,
aggregation streaming, Change Stream resume/filter/cancellation, and rejection of
wrong passwords, untrusted CAs and plaintext.

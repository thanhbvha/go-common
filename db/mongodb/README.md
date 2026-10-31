# MongoDB production APIs

The original client, manager and repository APIs remain available. New methods use
separate names, preserving existing interfaces and function-value signatures.
See [runnable examples](../../examples/db_mongodb/README.md) for complete fixtures.

## Startup and readiness

```go
cfg := mongodb.Config{
    URI: os.Getenv("MONGODB_URI"),
    DBName: "orders",
    ClientGroup: "business",
    RequireTransactions: true,
}
```

`RequireTransactions` makes `New`/manager startup read `hello` after ping and reject
standalone or unsupported topology with `ErrTransactionsUnsupported`. Leave it false
for CRUD-only applications. `DetectCapabilities(ctx, client)` and
`manager.Capabilities(ctx, alias)` provide fresh read-only topology/session/wire-version
information. This describes server support, not the authenticated principal's rights.
Capability probing is not added to every transaction's hot path.

`ConfigError` supports `errors.As` with `Field`/`Code`; `errors.Is(err, ErrInvalidConfig)`
classifies validation failures. `ClientGroupError.Fields` identifies differing policy
fields, without including their values. Credentials and raw URI parser errors are
never included in configuration errors. Other operational errors retain driver
contracts and must be sanitized before logging.

## Lifecycle and transaction draining

Lifecycle operations honor cancellation while waiting for another lifecycle operation,
and validate the context again before publishing a new client or binding.
`DisconnectAll` marks the manager closing, rejects new lookups/transactions with
`ErrManagerClosing`, waits for managed transactions through commit/session cleanup,
then closes each client once. A drain deadline leaves clients/bindings owned and
closing; retry `DisconnectAll` with a fresh context after active work ends. A successful
close clears bindings and permits `Init` again. Statistics are lifetime manager totals.

```go
shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
defer cancel()
err := manager.DisconnectAll(shutdownCtx)
// If draining timed out, clients are retained. Arrange a later retry or your
// application's explicit forced-exit policy; do not treat it as a completed close.
```

Stop accepting application requests before shutdown. Direct client operations and
sessions created outside manager transaction helpers are not tracked. Do not call
shutdown from inside its own active transaction callback. Callback cancellation is
cooperative. Driver commit/abort/session cleanup may extend past an operation
deadline; these contexts are not a guarantee that arbitrary callbacks stop on time.

## Transaction observability

```go
monitor := &mongodb.TransactionMonitor{
    Event: func(e mongodb.TransactionEvent) {
        // Feed a concurrency-safe metrics recorder. No URI/document/error payload
        // is supplied. Keep this fast; never call manager lifecycle methods here.
        if e.Phase == "finished" {
            recordDuration(e.ClientGroup, e.Outcome, e.Duration)
        }
    },
}
cfg.TransactionMonitor = monitor
stats := manager.TransactionStats()
```

Phases are `started`, `attempt`, `finished`. Final outcomes are `committed`, `failed`,
or `unknown`; an unresolved `UnknownTransactionCommitResult` must not be described
as a rollback. `CallbackAttempts`/`Retries` count callback execution, not commit retry
commands. Driver retry logic remains authoritative. Stats include started, committed,
failed, unknown, active and callback/retry counts. Hook panics are isolated; business
callback panics still propagate after cleanup. Observers must be fast, non-blocking,
concurrency-safe and must not reenter lifecycle operations. Share the same monitor
pointer across configurations in a `ClientGroup`.

## Bounded reads

```go
page, err := repo.PaginateCursor(ctx, authorizedFilter, mongodb.CursorPageRequest{
    Size: 50,
    After: previousPageCursor,
    Descending: true,
}, options.Find().SetProjection(bson.M{"_id": 1, "status": 1}))
// Continue with page.NextCursor only when page.HasMore is true.

err = repo.AggregateEach(ctx, pipeline, func(ctx context.Context, row *Row) error {
    return consume(ctx, row)
}, options.Aggregate().SetBatchSize(100))
```

Cursor pagination uses indexed `_id` range queries, with size default 10/cap 1000,
fetching size+1 without count or skip. The collection must have a uniform scalar
`_id` type. Sort/skip/limit/cursor type are controlled by this API. Projections must
retain `_id`. The opaque BSON/base64 token preserves ObjectID/numeric types, is limited
to 4096 characters and is rejected on malformed payload or direction mismatch.
Tokens are unsigned navigation positions: always authorize/reapply the domain filter;
keep filter, collation and direction unchanged between pages. They are not snapshot
or authentication tokens. Use domain-specific range queries for other sort fields.

`AggregateEach` streams repository `T`; `ExecuteAggregateEach[Output]` supports a
different pipeline output type through a native collection. The free function is
a native-driver escape hatch: scoped repositories retain transaction guards, while
callers of free functions/native collections must preserve the session themselves.
Callbacks stop on errors. Cursor cleanup uses an independent bounded context, and
cleanup errors are joined while preserving `errors.Is` for the primary error.
`FindAll` and `Aggregate` still materialize results for compatibility; use streaming
for large data. Count caps are not byte budgets; project large document fields out.

## Change Streams: watching future changes

`FindEach`/`AggregateEach` iterate query results. `Watch`/`WatchEach` subscribe to
collection changes on a replica set or supported sharded deployment. Both reuse
the repository's collection/client and forward `options.ChangeStream()` options.
The authenticated user needs the appropriate `find` and `changeStream` privileges.

```go
pipeline := mongo.Pipeline{
    bson.D{{Key: "$match", Value: bson.M{"operationType": "insert"}}},
}
err := repo.WatchEach(workerCtx, pipeline,
    func(ctx context.Context, change *mongodb.ChangeEvent[Notification]) error {
        if change.FullDocument == nil {
            return errors.New("notification document is missing")
        }
        return sender.Send(ctx, change.FullDocument) // Application-owned transport.
    }, options.ChangeStream().SetBatchSize(100).SetMaxAwaitTime(time.Second))
```

`WatchEach` blocks and invokes callbacks sequentially, providing natural
backpressure without spawning goroutines. Errors from callbacks/decode/driver
stop consumption; context cancellation remains detectable with `errors.Is`.
Cleanup runs with a fresh five-second budget even after cancellation, and its
errors are joined with the primary error. Callbacks must return and observe the
context; cancellation cannot interrupt arbitrary application code.

`ChangeEvent[T]` exposes `ResumeToken` (opaque BSON `_id`), `OperationType`,
`FullDocument *T`, `DocumentKey` (raw BSON), `Namespace` and `ClusterTime`.
`FullDocument` can be nil for deletes or updates without lookup. Insert events
already include it; request `SetFullDocument(options.UpdateLookup)` explicitly
when needed for updates. Pipeline nil/empty means all changes. Keep the event's
`_id` intact when projecting; do not confuse it with `fullDocument._id`.

For explicit startup coordination, open with `repo.Watch(...)`, then start writers,
iterate/decode the returned `*mongo.ChangeStream` and close it yourself. A watch
starts at its established position; opening after a write without a resume/start
option may miss that write. The raw API also exposes driver-specific event fields
not represented by `ChangeEvent`. Database/client-wide watching is available
through native handles; repository helpers watch one collection.

Use a worker lifetime context rather than a short HTTP request or transaction
context. Driver/client operation timeouts still apply; keep `MaxAwaitTime` below
the effective operation timeout. Transaction repositories and session contexts
with an active transaction are rejected with `ErrChangeStreamInTransaction`.
No manager lifecycle behavior changes: cancel workers, wait for them to return,
then call `DisconnectAll`. Transaction draining does not track watchers.

Driver automatic resume handles eligible transient errors within a running stream.
These helpers add no outer retry loop, persistent resume-token store, delivery
acknowledgement or automatic replay of callback failures. An unrecoverable stream
error is returned to the application's supervisor. Restarting without an explicit
resume/start option watches new changes rather than recovering missed events.
Expired resume history requires application recovery, not silently resetting the
position. Multiple application instances watching the same collection each receive
the changes; this is not consumer-group load balancing or an exactly-once transport.

See the self-cleaning [notification example](../../examples/db_mongodb/watch.go):
it filters inserts, maps ObjectID to an application payload, verifies an intermediate
update is filtered, and resumes with a token kept only in memory. It has no
Socket.IO dependency and no persistent checkpoint.

## Driver options with transaction guards

`FindByIDWithOptions`, `FindOneWithOptions`, `InsertOneWithOptions`,
`InsertManyWithOptions`, `InsertManyTyped`, `UpdateOneWithOptions`,
`UpsertOneWithOptions`, `UpdateManyWithOptions`, `DeleteOneWithOptions`,
`DeleteManyWithOptions`, `CountWithOptions`, `ExistsWithOptions`,
`BulkWriteWithOptions`, and `CreateIndexWithOptions` forward driver options while
retaining context/lifetime checks on transaction repositories. `InsertManyTyped`
accepts `[]T`. Existing methods keep their signatures.

```go
_, err := scoped.UpdateOneWithOptions(tx.Context(), filter, update,
    options.UpdateOne().SetArrayFilters([]any{bson.M{"item.quantity": bson.M{"$gt": 0}}}))
_, err = repo.InsertManyWithOptions(ctx, documents, options.InsertMany().SetOrdered(false))
```

`UpsertOneWithOptions` always enforces upsert=true; `ExistsWithOptions` retains `_id`
projection. Unordered bulk operations can partially succeed: inspect result and
error rather than blindly retrying. `Collection()` remains an explicit escape hatch,
not a safe substitute for scoped operations.

## Verification

[MongoDB CI](../../.github/workflows/mongodb.yml) uses the same disposable
[fixture script](../../.github/scripts/test-mongodb.sh) as local verification.
The replica mode tests three-node primary failover, standalone rejection, commit
retry, drain, cursor pagination, query streaming, Change Streams and options. Change
Stream tests cover filtering/order, callback failure, idle cancellation and driver
resume after a resumable getMore error. Secure mode verifies TLS/CA
and authentication, including negative connection attempts. Fixtures are local-only
and do not establish production throughput, backups, restore or availability guarantees.

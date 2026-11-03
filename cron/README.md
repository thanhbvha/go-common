# Cron scheduler

Operational maturity, defaults and adoption gates are tracked in the [production matrix](../docs/PRODUCTION.md).

In-process scheduling with six-field cron expressions (seconds first), optional
Redis leases, cancellation, panic recovery and execution observations. Scheduling
is **best effort**: there is no exactly-once delivery, durable retry queue or
catch-up after downtime. Make business writes idempotent.
Use `cron.ExecutionFromContext(ctx)` inside callbacks to obtain `JobName` and
`ScheduledAt` for a stable occurrence-based business key.

## API

| API | Purpose |
| --- | --- |
| `NewScheduler(options ...Option)` | Configure timezone, namespace, logger, observer and local overlap policy. |
| `AddJob(JobConfig)` | Register a named local job with `Run func(context.Context) error`. |
| `AddDistributedJob(DistributedConfig, LeaseProvider)` | Register a named distributed job with the same callback contract. |
| `Start()` | Begin dispatching jobs; repeated calls have no effect. |
| `Shutdown(context.Context) error` | Stop dispatch, cancel callbacks and wait within the caller's deadline. |
| `ExecutionFromContext(context.Context)` | Read the job name and scheduled timestamp for an idempotency key. |

Both registration methods return an entry ID and an error. Always check the error.
All callbacks must accept a context and return an error. Names are required and
unique within a scheduler, across local and distributed jobs.

## Local job

```go
scheduler := cron.NewScheduler(
    cron.WithLocation(time.UTC),
    cron.WithLogger(slog.Default()),
    cron.WithObserver(func(r cron.Result) {
        // Export counters/histograms; callbacks can run concurrently.
        slog.Info("job result", "job", r.JobName, "status", r.Status)
    }),
)
_, err := scheduler.AddJob(cron.JobConfig{
    JobName: "local-metrics",
    Schedule: "*/5 * * * * *",
    Timeout: 3 * time.Second,
    Run: func(ctx context.Context) error {
        if err := ctx.Err(); err != nil { return err }
        slog.InfoContext(ctx, "local metrics collected")
        return nil
    },
})
if err != nil { return err }
scheduler.Start()
```

Use local jobs for work that each application instance should perform. They skip
overlap by default; configure `WithOverlapPolicy(cron.AllowOverlap)` if concurrent
local executions are intentional.

## Distributed job

```go
rdb := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
    ContextTimeoutEnabled: true,
    DialTimeout: 2 * time.Second,
    ReadTimeout: time.Second,
    WriteTimeout: time.Second,
})
scheduler := cron.NewScheduler(
    cron.WithLocation(time.UTC),
    cron.WithNamespace("reports:production"),
    cron.WithLogger(slog.Default()),
    cron.WithObserver(func(r cron.Result) {
        // Export metrics here; this callback must be concurrency-safe and fast.
    }),
)
_, err := scheduler.AddDistributedJob(cron.DistributedConfig{
    JobName: "daily-report",
    Schedule: "0 0 8 * * *", // 08:00 UTC
    LockTTL: 30 * time.Second,
    LockTimeout: 3 * time.Second,
    DeduplicationTTL: 48 * time.Hour,
    Timeout: 10 * time.Minute,
    Run: func(ctx context.Context) error {
        if err := ctx.Err(); err != nil { return err }
        execution, ok := cron.ExecutionFromContext(ctx)
        if !ok { return errors.New("missing cron execution metadata") }
        key := execution.JobName + ":" + execution.ScheduledAt.UTC().Format(time.RFC3339Nano)
        // Enforce this key with a unique constraint/transaction in the real write.
        slog.InfoContext(ctx, "generating report", "idempotency_key", key)
        return nil
    },
}, cronredis.New(rdb))
if err != nil { return err }
scheduler.Start()
// On SIGTERM / SIGINT, before closing DB/Redis:
ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
defer cancel()
if err := scheduler.Shutdown(ctx); err != nil {
    // Callbacks may still be running. Keep dependencies open while waiting,
    // or let your process supervisor enforce the final termination deadline.
    return err
}
```

The snippets use `context`, `errors`, `log/slog`, `time`, `go-redis/v9` and the
`cron` and `cron/lease/redis` packages (alias `cronredis`). They belong inside an application function returning
`error`. See `examples/cron` for complete runnable programs.

## Execution and delivery contract

1. Each invocation receives its actual scheduled timestamp from the cron engine,
   even if its goroutine starts late. A unique ownership session acquires an
   execution lease at `<namespace>:lock:<job>`.
2. The holder claims `<namespace>:slot:<job>:<scheduled-unix-nanoseconds>` with
   SET NX and a retention TTL. A marker is kept after success, failure or panic;
   a late replica cannot repeat that slot while the marker exists. Namespace and
   job segments are URL query escaped in Redis keys.
3. The execution lease renews every `LockTTL/3`. Renewals are ownership checked;
   uncertainty, lost ownership or confirmed expiry cancels the job context.
   An independent timer also detects expiry if a Redis operation stalls.
4. The execution lease is released with ownership checking only after the
   callback returns. It continues renewing during timeout/shutdown cancellation
   while the callback is still running. Local overlap is always skipped for
   distributed jobs, including callbacks that ignore cancellation after lease loss.

`LockTTL` is **independent of the schedule interval** and must be at least 30ms.
Use seconds or longer with headroom for network latency and process pauses.
`LockTimeout` defaults to `min(LockTTL/6, 5s)` and must be less than `LockTTL/3`.
Enable `ContextTimeoutEnabled` and configure bounded Redis transport timeouts.

`DeduplicationTTL` defaults to 24h and must be at least `LockTTL`. Choose it to
cover the maximum tolerated delay/replay window. Redis stores approximately
`retention / interval` markers per job; expired markers permit duplicates from
very late nodes. Distinct scheduled slots are distinct runs, even if a replica
missed earlier slots. Relative `@every` schedules are rejected for distributed
jobs because replica startup times cannot define a shared slot; use calendar
expressions instead. Replicas must use matching schedules, timezones, names,
namespaces and synchronized clocks.

A failed lock operation or a busy lease skips that invocation. If the holder dies
between claiming a marker and committing business work, the slot may be lost.
Restarts do not replay missed slots. Redis failover, eviction, lost writes or a
callback ignoring cancellation can permit duplicates/overlap across replicas.
Use a dedicated non-evicting Redis deployment where appropriate, and enforce
idempotency/unique business keys or fencing at the database. If every slot must
be recovered, use a durable work queue/execution ledger with explicit retries;
this scheduler does not supply one and does not automatically retry side effects.

## Local jobs, observations and lifecycle

- `AddJob(JobConfig{JobName, Schedule, Timeout, Run})` supports context and errors.
- Local jobs skip overlapping invocations by default. Select
  `WithOverlapPolicy(cron.AllowOverlap)` to allow concurrent local execution.
- Distributed callbacks use `DistributedConfig.Run func(context.Context) error`.
- Negative timeouts, invalid/impossible schedules, duplicate names, nil callbacks,
  empty namespaces for distributed jobs, invalid lease settings, relative
  distributed schedules and nil locks are rejected during registration. A zero execution timeout means no deadline.
- Panics are recovered per execution and logged with stack traces. `Result`
  exposes job, scheduled/start times, duration, error and status. Status values:
  `success`, `failed`, `panic`, `timeout`, `canceled`, `lease_lost`,
  `skipped_running`, `skipped_lock`, `skipped_slot`. Observer panics are isolated.
  Observers must return promptly and must not synchronously wait for shutdown.
  Skip logs use debug level; expected shutdown cancellation uses info level.
  Cancellation remains visible as `Result{Status: "canceled", Err: context.Canceled}`
  (possibly wrapped); timeout, lease loss and business failures use error level.
  Observers should count canceled invocations separately from failures. Build success/failure/skip
  counters, duration histograms and last-success alerts from the observer.
- `Shutdown(ctx)` stops dispatch, cancels job contexts and waits for callbacks
  and cleanup.
  Its deadline bounds the caller's wait; Go cannot forcibly terminate callbacks.
  You may call `Shutdown` again to finish waiting. Shutdown is terminal;
  construct a new scheduler to restart. Registration after shutdown returns `ErrStopped`.
- `WithLocation` configures a timezone; the default is `time.Local`. Expressions may also use robfig's `CRON_TZ=...` prefix. Use the
  same timezone and include tzdata in images when using named locations.

## Application lifecycle and deployment

Register all jobs, call `Start()`, and handle SIGINT/SIGTERM with `Shutdown(ctx)`
before closing Redis/DB clients. A shutdown deadline bounds the caller's wait;
callbacks must honor context cancellation. If shutdown times out, keep resources
open while waiting again or let the supervisor enforce a hard process deadline.
Create a new scheduler to restart after shutdown.

Use a namespace specific to your service/environment. Do not mix scheduler
versions with different locking protocols when duplicate execution matters:
quiesce old schedulers, wait for running jobs/lease TTLs, then start the new
version. Changing a namespace, job name or schedule requires the same coordination
and may permit new execution.

See [the runnable examples](../examples/cron/README.md) for local and distributed
modes, bounded Redis transport settings and signal handling.

## Verification

```sh
go test -race -count=10 ./cron
go test -race ./cache -run '^TestRedisLockSession'
go vet ./cron ./cache ./examples/cron
```

Cron tests use isolated miniredis instances, independent schedulers and explicit
slot times for deterministic concurrency checks. Also validate transport failures,
failover and deployment shutdown against the actual Redis topology before rollout.


The core cron package uses a generic LeaseProvider and does not import cache or
Redis. Import `cronredis "github.com/thanhbvha/go-common/v2/cron/lease/redis"`
for Redis sessions. The adapter borrows its client; close it after scheduler drain.
Custom providers must honor operation contexts and create independent ownership
sessions. Session keys cannot be reacquired after success or an ambiguous result.

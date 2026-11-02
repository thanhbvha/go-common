# Cron examples

Run either mode from the repository root:

```sh
# Named local job: AddJob(JobConfig{Run: func(context.Context) error}).
go run ./examples/cron -mode=local

# Redis must be available at localhost:6379. Run in several terminals to
# exercise AddDistributedJob(DistributedConfig{Run: ...}, redisLock).
go run ./examples/cron -mode=distributed
```

Both modes use `NewScheduler`, register jobs with checked errors, call `Start`
and finish with `Shutdown(ctx)`. Every callback receives a context and returns an
error; there are no alternate callback or shutdown APIs.

The distributed example uses a renewable execution lease, schedule-slot markers,
UTC, a service/environment namespace, a context-aware callback and an error
observer. It reads `ExecutionFromContext(ctx)` to show an occurrence-based
idempotency key; a real business write must enforce that key in its database.
The lease TTL can exceed the cron interval; it is renewed while work continues. Busy executions skip later invocations rather than queueing them.

For an isolated Redis endpoint/namespace, add
`-redis-addr=127.0.0.1:6380 -namespace=reports:testing`.

Both examples handle SIGINT and SIGTERM with `Shutdown(ctx)` before dependencies
close. Interrupting an active job logs `status=canceled` at INFO, keeps the
cancellation error available to observers and does not report a business failure.
If the grace period expires, the example continues waiting to avoid closing
Redis under a running callback; the process supervisor sets any hard kill deadline.

Distributed scheduling is best effort. Write business operations with idempotency
keys; lease/slot markers do not provide exactly-once delivery, durable retries or
catch-up after downtime. See [the module contract](../../cron/README.md) for
retention, cancellation, rollout and Redis failure limitations.

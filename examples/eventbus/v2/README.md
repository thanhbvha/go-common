# EventBus v2 API walkthrough

Run from the repository root with the Go toolchain required by `go.mod`:

```powershell
go run ./examples/eventbus/v2
```

The program is finite and needs no external services or credentials. It uses a
dedicated memory transport, a ten-second overall deadline, and a fresh shutdown
deadline on every exit path. Memory data disappears when the process exits.

Expected output:

```text
v2 complete: batch=5 duplicate=true task_calls=8 event_groups=2 dlq=0
```

Read [main.go](main.go) in order:

| Stage | APIs and behavior |
| --- | --- |
| Open | `DefaultConfig`, explicit `WithTransport(memory.New())` |
| Declare | One task, one event, independent `audit` and `notification` groups |
| Handle | `Handle[T]`, payload validation, context cancellation, attempt limits |
| Publish | Stable `EnqueueWithResult` identity and confirmed duplicate |
| Batch | Five accepted results; error reports the accepted prefix |
| Outcomes | Success ACK, retry once, intentional skip, permanent failure to DLQ |
| Schedule | Relative `WithDelay` and absolute `WithRunAt` |
| Metadata | Header and a process-local concurrency key |
| Inspect | Wait for actual topology/subscriber statistics, bounded by context |
| Repair | Pause, drain task intake, list DLQ, fix synthetic failure, requeue by DLQ record ID, resume |
| Shutdown | Close and drain the bus with a fresh deadline, preserving cleanup errors |

The handler call count includes the retry and the repaired DLQ requeue. The two
event groups each receive the publication once in this local scenario. These
counts describe this controlled run, not an exactly-once production guarantee.

For durable side effects, replace the comment in the task handler with an
application transaction or durable idempotency check. The atomic repair flag is
only a local simulation of fixing a dependency; it is not a production repair
mechanism. Publication dedupe cannot prevent broker redelivery.

For Redis, NATS and Kafka, use the existing [broker examples](../README.md).
They include client construction, environment-based authentication, targeted
event-layout options, producer/worker/subscriber/admin commands and client
shutdown after bus drain. This memory walkthrough intentionally does not
pretend that changing a driver string provides those broker prerequisites.

For OpenTelemetry, batch admission and rate limits, see
[observability](../observability/main.go). For the full contract, migration
boundaries and capability differences, see the
[module guide](../../../eventbus/README.md) and
[reference](../../../eventbus/reference/README.md).

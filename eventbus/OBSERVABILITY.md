# EventBus observability hooks

Set `Config.Metrics` and `Config.Tracer` before calling `Open`. Hooks are called
synchronously from concurrent producers/workers: implementations must be
concurrency-safe, bounded, and nonblocking. Export through your application's
metrics/tracing stack; the core does not select a vendor or start a server.

## Metrics

Existing `Metrics` implementations remain valid:

| Hook | Meaning |
| --- | --- |
| `IncEnqueue` | One valid-name publish API call, with `success`, `error`, or `duplicate`; duplicate reservations are not broker acceptance. Validation/encoding/reservation/produce errors are counted. |
| `IncDequeue` | One delivery entering processing, including malformed envelopes and exhausted attempts. |
| `ObserveDuration` | Handler execution duration in seconds, including recovered panic and cooperative timeout. |
| `IncComplete` | `ack`, `retry`, or `dlq`, only after successful settlement confirmation. |

Implement the optional `RuntimeMetrics` interface on the same object to receive:

- `IncHandlerFailure`: `panic` or `timeout`. A handler that returns after its
  processing deadline is classified as timeout even if it returns nil. Timeout
  notification occurs when the handler returns; the engine does not spawn an
  unbounded goroutine to abandon uncooperative handlers.
- `ObserveSettlement`: decision, `success`/`error`, and operation latency in
  seconds. This includes ACK, retry, and DLQ failures; failed operations do not
  increment completion counters.
- `IncReconnect`: one replacement session after backoff, excluding initial
  startup and normal shutdown.
- `ObserveSaturation`: seconds blocked in an intake callback after its bounded
  queue is full, including a wait interrupted by intake cancellation.
- `ObserveQueueAge`: age of the delivery being processed since the later of
  envelope creation/scheduled time. This is a sample, not broker-wide oldest age.
- `ObserveStats`: successful explicit `Bus.Stats` reads, including lag and oldest
  age only when their `LagKnown`/`OldestAgeKnown` flags are true. Unknown is not
  zero. Redis calculates both measurements. NATS reports oldest age only when
  it can derive it without confusing retained acknowledged history with lag;
  Kafka reports oldest age only for an empty main-topic lag; nonempty age is
  unknown because offset metadata alone does not identify the oldest retained
  delivery obligation.

There is no hidden background stats poller. An application can call
`bus.Stats(ctx, "send_email")` at its preferred scrape interval. Unsupported
drivers return `ErrNotSupported` and emit no fabricated stats sample.

Redis statistics are a near-point-in-time snapshot assembled from multiple
commands. `Pending` means ready delivery obligations, `InFlight` means delivered
but unsettled pending entries, and `Lag` is their sum; delayed and DLQ records
are separate. Event values aggregate all `subscriber:*` consumer groups, while
task values use `workers`. `OldestAge` is the oldest ready/in-flight obligation,
using the later envelope creation/schedule time and falling back to the stream
ID timestamp. It is unknown when Redis reports outstanding state whose source
entry is no longer available. Redis 7 supplies group lag directly when possible;
Redis 6.2-compatible operation falls back to cancellable 1,000-entry stream scans,
so applications with large ready backlogs should avoid overly frequent polling.

NATS statistics are also near-point-in-time. `Pending` and `InFlight` come from
JetStream consumer `NumPending` and `NumAckPending`; event values sum every
durable subscriber consumer. A producer-only task has no consumer yet, so its
retained main-stream messages are reported as pending. Delayed values are
streamed from the topology KV key index and DLQ values come from DLQ stream
state. Acknowledged records retained by the main stream are not counted as lag.

Kafka statistics calculate lag from topic end offsets and committed consumer
group offsets. Kafka does not expose whether an uncommitted offset has already
been fetched, so the driver reports all main-topic lag as `Pending`, reports
`InFlight` as zero, and sets `Lag = Pending`. Delayed and logical DLQ counts are
separate. Event aggregation requires permission to list consumer groups. See
[driver/kafka/ADMIN.md](driver/kafka/ADMIN.md).

Names are registered topology names; subscriber processing retains the legacy
`event_subscriber` metric name. Do not attach job IDs, payloads, arbitrary headers,
or original error strings as metric labels. Runtime hooks carry fixed outcomes.

## Tracing and propagation

The existing `Tracer.Start` hook receives operations `eventbus.publish`,
`eventbus.process`, and `eventbus.settle.ack|retry|dlq`. Each started span ends on
success, handler failure, or settlement error. Handler contexts inherit the
processing span; settlement spans are its children and remain alive during
graceful drain. Forced cancellation interrupts them.

Implement optional `TraceInjector.Inject(ctx, carrier)` to inject the producer
span's propagation headers before durable envelope encoding. Implement
`TraceRecorder.RecordEvent` to receive sanitized publish, panic, timeout, and
settlement outcome events. Original errors/panic values and payloads are never
added to these events. Panic source stacks remain in bounded logs (P2.5).

Processing receives a copied envelope carrier. Retry preserves envelope headers
and also supplies them in `Decision.Next.Headers`; DLQ retains the original
serialized envelope with its headers. This includes `traceparent`, `tracestate`,
and consumer-defined correlation headers without hard-coding a tracing vendor.
Carriers are copied so producer injection does not mutate caller-owned maps.
Consumers should use synthetic/non-sensitive correlation IDs and never put
credentials or personal data in propagation headers.

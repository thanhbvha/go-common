# EventBus operations guide

EventBus exposes vendor-neutral hooks; the application owns its OpenTelemetry
SDK, exporter, dashboard, and alert routing. The runnable adapter in
`examples/eventbus/observability` maps every core hook to a bounded set of OTel
metrics and propagates W3C trace context without using payloads, IDs, headers,
or error strings as labels.

## Run the observable example

Start an OTLP gRPC collector on `127.0.0.1:4317`, then run:

```powershell
go run ./examples/eventbus/observability --driver memory --otel-endpoint 127.0.0.1:4317
```

The program starts a task consumer, pauses intake, batch-enqueues two messages,
drains the local topology, resumes it, waits for confirmed completion, samples
broker statistics, and gracefully drains. Broker-backed drivers accept the same
flags documented in `examples/eventbus/README.md`.

The example emits these OTel instruments:

| Instrument | Kind | Important attributes |
| --- | --- | --- |
| `eventbus.publish` | counter | `eventbus.topology`, `eventbus.outcome` |
| `eventbus.delivery` | counter | `eventbus.topology` |
| `eventbus.completion` | counter | `eventbus.topology`, `eventbus.decision` |
| `eventbus.handler.duration` | histogram, seconds | `eventbus.topology` |
| `eventbus.handler.failure` | counter | `eventbus.topology`, `eventbus.outcome` |
| `eventbus.settlement.duration` | histogram, seconds | topology, decision, outcome |
| `eventbus.reconnect` | counter | `eventbus.topology` |
| `eventbus.saturation.duration` | histogram, seconds | `eventbus.topology` |
| `eventbus.queue.age` | histogram, seconds | `eventbus.topology` |
| `eventbus.topology.messages` | gauge | topology and `eventbus.state` |
| `eventbus.topology.oldest.age` | gauge, seconds | `eventbus.topology` |

OTel Prometheus translation normally converts dots to underscores, adds unit
suffixes, and appends `_total` to counters. Confirm the exact translated names
against your collector/exporter before importing the included Grafana dashboard
at `eventbus/dashboards/grafana-eventbus.json`.

## Dashboard and alerts

The dashboard covers publish/completion rate, handler latency, failures,
settlement errors, reconnects, saturation, queue age, and the latest explicit
stats sample. Configure a service/namespace filter in the collector or dashboard
when multiple deployments share a backend.

Recommended initial alerts need workload-specific thresholds:

- backlog or oldest age increases for 15 minutes while completion rate is flat;
- DLQ count is nonzero or increases;
- settlement error or reconnect rate is nonzero for five minutes;
- handler p95 approaches `ProcessTimeout`;
- saturation p95 remains nonzero and completion throughput stops scaling;
- publish error ratio exceeds the application's agreed SLO.

`Bus.Stats` is not polled internally. Call it at a bounded interval to refresh
topology gauges. Treat `LagKnown=false` and `OldestAgeKnown=false` as unknown,
not zero. Redis stats may scan old-server backlogs, NATS values are consumer
snapshots, and Kafka cannot distinguish fetched from pending lag; see
`OBSERVABILITY.md` for exact semantics.

## Trace triage

Search for `eventbus.publish`, `eventbus.process`, and
`eventbus.settle.ack|retry|dlq`. A normal path has publish followed by process
and one confirmed settlement span. Missing settlement or repeated process spans
usually indicates lease loss, cancellation, ambiguous broker confirmation, or
an idempotent redelivery. Use stable business observability outside EventBus to
correlate side effects; never add payloads, credentials, arbitrary headers, or
high-cardinality job IDs as span attributes or metric labels.

Pause, resume, rate limits, and ordering-key limits are process-local. During an
incident, pause every affected instance or use broker-native controls when a
global stop is required. `DrainTopology` waits only for accepted local work and
does not stop producers or other instances.

# EventBus production-scale benchmark

This fixture supports P6.7.6 EB-13 evidence collection. It is disposable,
synthetic, and isolated from developer brokers. The tooling does not claim that
any evidence run or release gate passed; retain and review generated artifacts.

## Runner modes

`examples/eventbus/benchmark` supports two bounded modes:

- Finite mode uses `--messages N`. `--preload` pauses local intake, publishes the
  entire finite backlog, drains accepted work, then resumes for measurement.
- Sustained mode uses `--duration D --rate R` without `--preload`. Production is
  paced at the requested messages per second and any accepted backlog is drained
  before the result is emitted. `--timeout` must exceed `--duration`.

Both modes use synthetic payloads. Latency uses a fixed 512-bucket logarithmic
histogram. Exact duplicate-success detection uses one bit per possible sequence,
and the runner rejects configurations that would require tracking more than
100 million messages. The notification channel and producer in-flight window are
bounded. Enqueue timestamps travel in payloads rather than an in-memory slice.

`--samples-jsonl PATH` exclusively creates periodic JSONL containing produced and
completed counts, observed throughput, Go heap, goroutines, and topology statistics.
Unavailable topology statistics are marked with
`topology_stats_available: false`. Each sample records `phase: preload` or
`phase: measurement`. A preload run stops the preload sampler, resets Go heap,
CPU, elapsed-time, and peak baselines, and starts a new measurement sampler
immediately before resume. Stdout remains one final JSON object.

For Kafka, `--second-worker-after D` opens a second independent runtime and bus
with the same namespace and task handler. Both workers share the process-local
completion bitmap, so a consumer-group rebalance remains covered by unique-count
and duplicate verification. The result records whether the second worker
actually started and whether production or backlog was still active then; the
orchestrator rejects configuration-only evidence. It cannot be combined with preload, because pause is
local to one bus and the second worker could consume the staged backlog. External
worker restart is deliberately not offered:
restarting the only verifier process would discard exact completion state.
Kafka benchmark topologies provision one partition per requested concurrency
slot; the manifest and result record the effective partition count. This avoids
claiming concurrency 10 or 100 from the demo default of three partitions.
NATS benchmark topologies explicitly use and record `nats_delay_batch_size=1000`,
the driver's validated maximum. The reusable demo default remains 100. This
prevents a one-second scheduler tick with a 100-record batch from turning a
large delayed evidence cell into an accidental fixed 100 msg/s configuration
test while keeping the production tuning visible in artifacts.
Redis benchmark topologies likewise use and record
`redis_delay_batch_size=1000`, its validated maximum. This applies to delayed
and retry transfers without changing the reusable library or demo default.

## Disposable brokers

`eventbus/testdata/benchmark/compose.yaml` defines profile-scoped pinned images:

| Profile | Image | Container data path |
| --- | --- | --- |
| `redis` | `redis:7.4.1-alpine` | `/data` |
| `nats` | `nats:2.10.22-alpine` | `/data` |
| `kafka` | `apache/kafka:3.8.0` | `/var/lib/kafka/data` |

The orchestrator allocates run-specific loopback ports, a unique guarded Compose
project, a unique EventBus namespace, and named volumes labeled by that project.
It never accepts a remote endpoint. Do not adapt this fixture to shared brokers.

## Single run

Ubuntu/Linux uses the native Bash runner:

```bash
./eventbus/testdata/benchmark/run-production-scale.sh \
  --driver redis --workload delayed --messages 100000 --preload \
  --concurrency 10 --payload-kb 1 --delay 30s
```

Run a finite backlog from the repository root:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  ./eventbus/testdata/benchmark/run-production-scale.ps1 `
  -Driver redis -Workload delayed -Messages 100000 -Preload `
  -Concurrency 10 -PayloadKB 1 -Delay 00:00:30
```

Run a sustained workload with one scoped broker restart:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  ./eventbus/testdata/benchmark/run-production-scale.ps1 `
  -Driver nats -Workload immediate -Duration 02:00:00 -Rate 5000 `
  -Timeout 03:00:00 -BrokerRestartAfter 00:30:00
```

Exercise a Kafka consumer-group rebalance by starting a second worker in the same
benchmark process:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  ./eventbus/testdata/benchmark/run-production-scale.ps1 `
  -Driver kafka -Workload immediate -Duration 01:00:00 -Rate 2000 `
  -KafkaSecondWorkerAfter 00:10:00 -Timeout 02:00:00
```

`-Preload` is an explicit switch, not an orchestrator default. At most one broker
restart is scheduled unless `-BrokerRestartEvery`/`--broker-restart-every` is
set with an initial restart time in duration mode. Repeated runs fail unless the
expected number of actions occurs. Restart uses the run's Compose project and
service, never a container name or global Docker operation.

Each benchmark enqueue attempt is bounded to five seconds while retries remain
bounded by the whole-run context. This prevents one broker publish request that
straddles an outage from retaining the complete run deadline. Retry workloads
also record the expected retry set, unique candidates that failed at attempt one,
all synthetic attempt-one failure invocations, and retry candidates first
observed after broker attempt one. Acceptance requires complete unique retry-set
coverage and exact handler accounting, including repeated failures and observed
duplicates.

The configured restart duration is an earliest eligible time, not an unconditional
process-relative timer. The runner waits for an actual benchmark sample with at
least one produced message and active production or backlog before restarting the
broker. A benchmark that ends without reaching that condition fails the requested-
fault postcondition.

Duration mode does not replay missed producer slots after an outage. The result's
`messages` field is the number actually accepted for publication and must equal
the unique completion count. `duration * rate` is a pacing upper bound, not a
loss assertion when broker downtime delays an enqueue attempt past its next slot.

## Artifacts

Each invocation creates a collision-resistant directory under
`eventbus/testdata/benchmark/artifacts/` and refuses an existing path:

- `manifest.json` records dimensions, mode, ports, pinned fixture location, tool
  versions, synthetic/shared-broker declarations, namespace, and cleanup command.
- `broker-inspect.json` captures the broker image ID, image/configuration, mounts,
  labels, and runtime settings reported by Docker.
- `broker.log` captures scoped Compose service logs after startup, or immediately
  when startup/health validation fails.
- `result.json` is the runner's final result, including final pending, delayed,
  in-flight, DLQ, and lag counts read after topology drain and before transport
  close, plus retry and handler-accounting fields. A normal run fails unless all
  final obligations are zero and the accounting equations hold.
- `benchmark-samples.jsonl` contains process and topology samples.
- `broker-samples.jsonl` contains host process samples, Docker stats, the current
  benchmark phase, and named-volume allocated filesystem bytes measured inside
  the broker container from `du -sk` 1 KiB blocks. This avoids counting sparse
  Kafka segment apparent sizes as allocated disk usage.
- `fault-actions.jsonl` records the scoped broker restart timestamp, exit code,
  and produced/completed/lag state around the action. A requested restart must
  occur while production or backlog is active. Kafka second-worker configuration
  and observed startup state are represented by the manifest and final result.
- `summary.json` validates result identity, final settled state, requested fault
  postconditions, and JSON/JSONL parsing; it separates `execution_status` from
  `acceptance_status`, records preload/measurement/volume sample counts, maximum
  named-volume bytes, selected maxima, and cleanup status, and reports whether a
  still-running benchmark child had to be forcibly stopped. Acceptance fails if
  no measurement-phase or numeric named-volume sample exists; preload runs also
  require at least one preload-phase sample.
  Repeated-restart runs also record `broker_restart_count` and fail unless it
  matches the cadence implied by duration, initial restart, and restart interval.
- `benchmark.stderr.log` contains diagnostics without credentials.

On orchestration failure, cleanup forcibly terminates and joins the benchmark
child before broker teardown. Cleanup then runs `down -v --remove-orphans` for the generated project. The
script then queries Docker project labels and fails unless no matching containers
or volumes remain. No global prune, `FLUSHDB`, broker-wide deletion, or unrelated
topic/key cleanup is used.

## Selected matrix

`examples/eventbus/benchmark/run-matrix.ps1` accepts comma-delimited durable
drivers, workloads, payload sizes, concurrency levels, and finite sizes. It validates
each expanded value and provides `-ValidateOnly` for Docker-free CLI checks. It invokes the
isolated orchestrator for each cell, places all runs below a new unique matrix
directory, and appends `matrix.jsonl`; neither run artifacts nor the index are
overwritten.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  ./examples/eventbus/benchmark/run-matrix.ps1 `
  -Drivers redis,nats -Workloads immediate,retry10 `
  -PayloadKB 1,10 -Concurrency 10 -Messages 100000,1000000 -Preload
```

Ubuntu/Linux equivalent:

```bash
./examples/eventbus/benchmark/run-matrix.sh \
  --drivers redis,nats --workloads immediate,retry10 \
  --payload-kb 1,10 --concurrency 10 --messages 100000,1000000 --preload
```

Use `-Duration` and `-Rate` for a sustained matrix. Duration mode accepts exactly
one `-Messages` value because that dimension does not control produced count.

## Validation and review

Parser and parameter validation does not require Docker:

```bash
./eventbus/testdata/benchmark/test-scripts.sh
```

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  ./eventbus/testdata/benchmark/test-scripts.ps1
```

The Bash runner preserves argument boundaries for artifact paths containing
spaces, terminates its child before Compose teardown, and samples the broker's
named-volume data path with `du` rather than treating container writable-layer
size as persistent broker storage.

Acceptance review should confirm completed unique messages equal produced
messages, duplicates/DLQ outcomes match the fault window, latency is meaningful
for the workload, heap/goroutines/broker memory/disk/lag remain bounded or drain,
and `summary.json` reports `acceptance_status: passed` and
`cleanup_verified: true`. Generated artifacts are
evidence inputs only; their presence is not proof that these criteria passed.

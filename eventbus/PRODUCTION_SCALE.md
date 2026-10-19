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

`--samples-jsonl PATH` exclusively creates periodic JSONL containing completed
count, observed throughput, Go heap, goroutines, and topology statistics.
Unavailable topology statistics are marked with
`topology_stats_available: false`. Stdout remains one final JSON object.

For Kafka, `--second-worker-after D` opens a second independent runtime and bus
with the same namespace and task handler. Both workers share the process-local
completion bitmap, so a consumer-group rebalance remains covered by unique-count
and duplicate verification. It cannot be combined with preload, because pause is
local to one bus and the second worker could consume the staged backlog. External
worker restart is deliberately not offered:
restarting the only verifier process would discard exact completion state.

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
restart is scheduled per run. Restart uses the run's Compose project and service,
never a container name or global Docker operation.

## Artifacts

Each invocation creates a collision-resistant directory under
`eventbus/testdata/benchmark/artifacts/` and refuses an existing path:

- `manifest.json` records dimensions, mode, ports, pinned fixture location, tool
  versions, synthetic/shared-broker declarations, namespace, and cleanup command.
- `broker-inspect.json` captures the broker image ID, image/configuration, mounts,
  labels, and runtime settings reported by Docker.
- `result.json` is the runner's final result.
- `benchmark-samples.jsonl` contains process and topology samples.
- `broker-samples.jsonl` contains host process samples, Docker stats, and the
  container writable-layer byte count from `docker inspect --size`.
- `fault-actions.jsonl` records the scoped broker restart timestamp and exit code
  when requested. Kafka second-worker configuration and outcome are represented
  by the manifest and final result.
- `summary.json` validates result identity and JSON/JSONL parsing, embeds the
  result, records sample counts and selected maxima, and records cleanup status.
- `benchmark.stderr.log` contains diagnostics without credentials.

Cleanup always runs `down -v --remove-orphans` for the generated project. The
script then queries Docker project labels and fails unless no matching containers
or volumes remain. No global prune, `FLUSHDB`, broker-wide deletion, or unrelated
topic/key cleanup is used.

## Selected matrix

`examples/eventbus/benchmark/run-matrix.ps1` accepts arrays for durable drivers,
workloads, payload sizes, concurrency levels, and finite sizes. It invokes the
isolated orchestrator for each cell, places all runs below a new unique matrix
directory, and appends `matrix.jsonl`; neither run artifacts nor the index are
overwritten.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  ./examples/eventbus/benchmark/run-matrix.ps1 `
  -Drivers redis,nats -Workloads immediate,retry10 `
  -PayloadKB 1,10 -Concurrency 10 -Messages 100000,1000000 -Preload
```

Use `-Duration` and `-Rate` for a sustained matrix. `-Messages` still labels each
selected cell but does not control produced count in duration mode.

## Validation and review

Parser and parameter validation does not require Docker:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  ./eventbus/testdata/benchmark/test-scripts.ps1
```

Acceptance review should confirm completed unique messages equal produced
messages, duplicates/DLQ outcomes match the fault window, latency is meaningful
for the workload, heap/goroutines/broker memory/disk/lag remain bounded or drain,
and `summary.json` reports `cleanup_verified: true`. Generated artifacts are
evidence inputs only; their presence is not proof that these criteria passed.

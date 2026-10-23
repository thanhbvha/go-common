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

`--samples-jsonl PATH` exclusively creates periodic JSONL. Process records use
`type: sample` and contain produced/completed counts, observed throughput, Go
heap, and goroutines. When enabled, topology records use
`type: topology_sample` and contain pending, delayed, in-flight, DLQ, and lag.
The two samplers have independent timestamps and run concurrently so a slow
broker stats request cannot block heap peak sampling. Each record includes
`phase: preload` or `phase: measurement`. A preload run stops the preload
samplers, resets Go heap, CPU, elapsed-time, and peak baselines, and starts new
measurement samplers immediately before resume. Stdout remains one final JSON
object.

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
Kafka benchmark topologies also explicitly use and record
`kafka_delay_batch_size=1000`, the driver's validated maximum. The reusable
library and demo default remains 100. This prevents the five-second scheduler
interval and 100-record default batch from becoming an accidental large delayed
backlog throughput ceiling.
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

| Profile | Immutable image | Container data path |
| --- | --- | --- |
| `redis` | `redis:7.4.1-alpine@sha256:c1e88455c85225310bbea54816e9c3f4b5295815e6dbf80c34d40afc6df28275` | `/data` |
| `nats` | `nats:2.10.22-alpine@sha256:73b0ba5fec5518c5f698597c58d2a3350a2b5ccae43e84c308f8d2da1242deca` | `/data` |
| `kafka` | `apache/kafka:3.8.0@sha256:c89f315cff967322c5d2021434b32271393cb193aa7ec1d43e97341924e57069` | `/var/lib/kafka/data` |

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
- `broker.log` captures scoped Compose service logs after startup and refreshes
  them immediately before teardown, or captures them immediately when
  startup/health validation fails.
- `result.json` is the runner's final result, including final pending, delayed,
  in-flight, DLQ, and lag counts read after topology drain and before transport
  close, plus retry and handler-accounting fields. It also records heap and
  goroutines after a synchronous post-drain GC so finite-run retained resources
  can be distinguished from the last in-flight periodic sample. A normal run
  fails unless all final obligations are zero, the post-drain metrics are present,
  and the accounting equations hold.
- `benchmark-samples.jsonl` contains independent `sample` process records and,
  when enabled, `topology_sample` broker-topology records.
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

Periodic topology sampling can be disabled with `--sample-topology=false` or
`-SampleTopology $false` while resource and broker sampling remain enabled. This
is intended for very large NATS delayed backlogs, where exact `Stats` must list
live KV keys and repeated scans would become part of the workload under test.
The mode is recorded in manifest and result and must match. Final exact topology
stats are always required after drain, so disabling periodic scans does not relax
the zero-obligation acceptance gate.

NATS KV snapshots used by scheduler and statistics code are implemented with
bounded ephemeral pull consumers. Do not replace them with callback `Watch` or
`ListKeys` paths that return before callback delivery has fully stopped: under
concurrent retry writes, those APIs can leave callbacks blocked on their update
channel and retain message buffers after drain. Production evidence must review
`goroutines.txt` for blocked KV callbacks in addition to checking numeric
post-drain metrics.
- `benchmark.stderr.log` contains diagnostics without credentials.
- `heap.pprof` is captured after final topology drain and a synchronous GC. It is
  required by the canonical orchestrators and supports retained-heap diagnosis
  without changing the measurement throughput window.
- `goroutines.txt` captures the post-drain goroutine stacks at the same boundary
  and is likewise required by the canonical orchestrators.

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

The PowerShell validation command is an optional Windows tooling compatibility
check, not a production-evidence release gate. Running it on an actual Windows
host validates process argument quoting, `.exe` invocation, path handling, and
Docker Desktop integration.

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

The accepted Redis 1m delayed/preload reference cell used a 1 KiB payload,
concurrency 100, a five-second delay, `redis_delay_batch_size=1000`, and periodic
topology sampling. It completed 1,000,000 unique messages with zero duplicates
and zero final obligations at 1,100.91 messages/second. Allocated Redis data
peaked at 884,355,072 bytes and ended at 230,834,176 bytes; cleanup was verified.
The artifact is under
`/tmp/thothan/p676-production-evidence/20261021T070920961Z-redis-delayed-1000000-redis-1m-delayed-preload-allocated-7b48cf4d/`.
This is one accepted matrix cell, not evidence that the full matrix passed.

The accepted Redis 1m `retry50` reference cell used a 1 KiB payload, concurrency
100, `redis_delay_batch_size=1000`, and periodic topology sampling. It completed
1,000,000 unique messages with all 500,000 expected retry failures, exactly
1,500,000 handler invocations, zero duplicates, and zero final obligations at
657.87 unique messages/second. Allocated Redis data peaked at 549,867,520 bytes
and ended at 359,661,568 bytes; cleanup was verified. The artifact is under
`/tmp/thothan/p676-production-evidence/20261021T080001972Z-redis-retry50-1000000-redis-1m-retry50-df712b70/`.
This is one accepted retry cell, not evidence that the full matrix passed.

The accepted Redis 1m `retry10` reference cell used a 1 KiB payload, concurrency
100, `redis_delay_batch_size=1000`, and periodic topology sampling. It completed
1,000,000 unique messages with all 100,000 expected retry failures, exactly
1,100,000 handler invocations, zero duplicates, and zero final obligations at
3,256.32 unique messages/second. Allocated Redis data peaked/finalized at
470,380,544 bytes; cleanup was verified. The artifact is under
`/tmp/thothan/p676-production-evidence/20261021T091703321Z-redis-retry10-1000000-redis-1m-retry10-dba56d28/`.
Together with the accepted immediate, delayed, and retry50 cells, this completes
the Redis 1m workload set only at the canonical 1 KiB/concurrency-100 dimension.

The accepted Kafka 1m delayed/preload reference cell used a 1 KiB payload,
concurrency/partitions 100, a five-second delay, `kafka_delay_batch_size=1000`,
and periodic topology sampling. It completed 1,000,000 unique messages with zero
duplicates and zero final obligations at 206.98 messages/second. Measurement
heap averaged 334.20 MB, peaked at 452.22 MB, and returned to 8.25 MB after drain;
the high-frequency peak was 499.37 MB. Allocated Kafka data peaked at 885,276,672
bytes and ended at 848,666,624 bytes; cleanup was verified. The artifact is under
`/tmp/thothan/p676-production-evidence/20261021T095244081Z-kafka-delayed-1000000-kafka-1m-delayed-preload-bcfaa9da/`.
The accepted correctness result also records nonlinear scheduler throughput and
backlog-proportional client heap as production tuning concerns.

The accepted NATS 1m immediate/preload reference cell used a 1 KiB payload,
concurrency 100, `nats_delay_batch_size=1000`, and periodic topology sampling. It
completed 1,000,000 unique messages with zero duplicates and zero final
obligations at 32,322.30 messages/second. Measurement heap averaged 11.36 MB,
peaked at 13.53 MB, and ended at 9.40 MB. Allocated JetStream data
peaked/finalized at 1,520,283,648 bytes; cleanup was verified. The artifact is
under
`/tmp/thothan/p676-production-evidence/20261022T022746582Z-nats-immediate-1000000-nats-1m-immediate-preload-1617f76a/`.
The 30.94-second drain yielded seven measurement resource samples and four
measurement broker samples, all accepted by the phase and evidence checks.

The accepted NATS 1m `retry10` reference cell used a 1 KiB payload, concurrency
100, `nats_delay_batch_size=1000`, and periodic topology sampling. It completed
1,000,000 unique messages with all 100,000 expected retry failures, exactly
1,100,000 handler invocations, zero duplicates, and zero final obligations at
6,717.80 unique messages/second. Sampled heap averaged 33.80 MB, peaked at
71.70 MB, and ended at 49.54 MB; the high-frequency peak was 76.08 MB. One of 30
periodic topology reads was transiently unavailable, while subsequent and final
exact stats passed. Allocated JetStream data peaked/finalized at 1,844,441,088
bytes; cleanup was verified. The artifact is under
`/tmp/thothan/p676-production-evidence/20261022T023734743Z-nats-retry10-1000000-nats-1m-retry10-e59b4629/`.
Its rising finite-run heap windows remain documented and are not substituted for
the separate sustained-soak evidence.

The accepted Kafka 1m `retry50` reference cell used a 1 KiB payload,
concurrency/partitions 100, `kafka_delay_batch_size=1000`, and periodic topology
sampling. It completed 1,000,000 unique messages with all 500,000 expected retry
failures, exactly 1,500,000 handler invocations, zero duplicates, and zero final
obligations at 4,124.16 unique messages/second. Sampled heap averaged 12.20 MB,
peaked at 15.67 MB, and ended at 8.48 MB. Allocated Kafka data
peaked/finalized at 734,973,952 bytes; cleanup was verified. The artifact is under
`/tmp/thothan/p676-production-evidence/20261022T005504565Z-kafka-retry50-1000000-kafka-1m-retry50-7c9299ec/`.
Together with retry10, this completes the Kafka 1m retry workloads only at the
canonical 1 KiB/concurrency-100 dimension.

The accepted Kafka 1m immediate/preload reference cell used a 1 KiB payload,
concurrency/partitions 100, and periodic topology sampling. It completed
1,000,000 unique messages with zero duplicates and zero final obligations at
4,177.66 messages/second. Measurement heap averaged 330.31 MB, peaked at
442.72 MB, and ended at 32.38 MB; the high-frequency peak was 536.87 MB and
tracked the staged pending backlog rather than retained post-drain growth.
Allocated Kafka data peaked at 570,462,208 bytes and ended at 505,024,512 bytes;
cleanup was verified. The artifact is under
`/tmp/thothan/p676-production-evidence/20261022T020915669Z-kafka-immediate-1000000-kafka-1m-immediate-preload-9211c10c/`.
Together with delayed, retry10, and retry50, this completes the Kafka 1m workload
set only at the canonical 1 KiB/concurrency-100 dimension.

The accepted Kafka 100k immediate/preload reference cell used a 1 KiB payload,
concurrency/partitions 100, and periodic topology sampling. It completed 100,000
unique messages with zero duplicates and zero final obligations at 4,848.85
messages/second. Peak heap was 286,731,512 bytes and returned to 3,417,848 bytes
after drain; post-drain profiles showed only expected Kafka lifecycle goroutines
and no blocked application stack. Allocated Kafka data peaked at 77,778,944
bytes across 26 samples and cleanup was verified. The artifact is under
`/tmp/thothan/p676-production-evidence/20261022T051247081Z-kafka-immediate-100000-kafka-100k-immediate-preload-startup-fixed-final-1d9d4f4f/`.
A cold-fixture diagnostic showed that creating five 100-partition topics can
exceed 20 seconds, so the benchmark runner uses a two-minute Kafka startup
deadline without changing library or demo defaults.

The accepted Kafka 100k delayed/preload reference cell used a 1 KiB payload,
concurrency/partitions 100, a five-second delay, and
`kafka_delay_batch_size=1000`. It completed 100,000 unique messages with zero
duplicates and zero final obligations at 204.75 messages/second. Preload ended
at 97,182 delayed plus 2,818 pending/lag and all 511 topology samples were
available. Measurement heap averaged 126.48 MB, peaked at 180.76 MB, ended at
7.50 MB, and returned to 3.54 MB after drain. Allocated Kafka data
peaked/finalized at 156,930,048 bytes across 241 samples; cleanup was verified.
The artifact is under
`/tmp/thothan/p676-production-evidence/20261022T052034500Z-kafka-delayed-100000-kafka-100k-delayed-preload-final-b365233f/`.
The near-identical 100k and 1m delayed throughput records a stable scheduler
ceiling and production tuning concern rather than nonlinear volume degradation.

The accepted Kafka 100k `retry10` reference cell used a 1 KiB payload,
concurrency/partitions 100, and `kafka_delay_batch_size=1000`. It completed
100,000 unique messages with all 10,000 expected retry candidates failing
exactly once at attempt one, exactly 110,000 handler invocations, zero
duplicates, and zero final obligations at 5,157.48 messages/second. Sampled heap
averaged 5.02 MB, peaked at 7.61 MB, ended at 4.46 MB, and returned to 3.42 MB
after drain. Allocated Kafka data peaked/finalized at 82,849,792 bytes across 14
samples; cleanup was verified. The artifact is under
`/tmp/thothan/p676-production-evidence/20261022T054124064Z-kafka-retry10-100000-kafka-100k-retry10-final-35219fac/`.
This completes the canonical 24-cell matrix only at the documented
1 KiB/concurrency-100 dimension. The fixture images are pinned by immutable
digest; broader dimensions and cross-platform PowerShell execution are optional
follow-ups rather than P6.7.6 gates.

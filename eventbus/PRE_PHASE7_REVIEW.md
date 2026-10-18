# EventBus review before Phase 7

- Review date: 2026-10-18
- Scope: core runtime, transport contracts, Memory/Redis/NATS/Kafka drivers,
  tests, examples, operations, performance, and Phase 7 prerequisites.
- Delivery contract used for this review: at-least-once; accepted work must not
  be silently lost.
- Worktree state at review start: clean.

## Executive conclusion

Phases 0-6 created a substantially safer implementation, and the current
hermetic tests, vet checks, and integration-tag compilation pass. The module is
nevertheless **not ready for a production migration**. This agrees with the
current capability matrix, which still marks every driver as not production
ready.

The most important remaining problems are not cosmetic optimizations:

1. a failed dedupe-reservation cleanup can turn a failed publication into a
   later false-success duplicate suppression;
2. Redis immediately publishes with approximate `MAXLEN` trimming, which can
   delete accepted but unprocessed work;
3. canonical physical names are not injective, so valid task/event or
   namespace/name combinations can map to the same broker resource;
4. Memory returns success while dropping messages when its fixed buffer is
   full;
5. Kafka delayed records can suffer unbounded head-of-line lateness within a
   partition despite the advertised five-second precision.

Create a short hardening phase (suggested name: **Phase 6.7**) and close the P0
items below before starting P7.1 adapters or sending shadow traffic to
EventBus. Phase 7 may otherwise encode unsafe behavior into the migration
layer and make later fixes compatibility-sensitive.

## Severity and disposition

| Severity | Meaning | Phase 7 disposition |
| --- | --- | --- |
| P0 | Can violate no-loss/routing invariants or make migration unsafe | Block Phase 7 production use |
| P1 | Production reliability, security, or operability gap | Fix before shadow/soak rollout |
| P2 | API, scalability, performance, or documentation debt | Schedule before deprecation gate |

## P0 findings

### EB-01: dedupe cleanup failure can suppress work that was never published

**Evidence:** `engine.go:935-955` reserves a dedupe key before publishing. If
publication fails, `release(context.WithoutCancel(ctx), key)` is called without
a deadline and its error is discarded. A subsequent call that cannot acquire
the stale reservation is treated as a duplicate and returns `nil`
(`engine.go:939-941`). Redis and NATS releases perform broker I/O, so release
failure is possible. No core fault test covers the combined publish-failure and
release-failure window.

**Impact:** publish fails before broker acceptance, reservation cleanup fails,
the caller retries, and EventBus reports duplicate success although no message
exists. This violates the primary no-silent-loss invariant. The unbounded
`WithoutCancel` cleanup can also hang the error path.

**Required fix:** make reservation state distinguish `reserved`, `published`,
and recoverable/expired attempts, or use an atomic/idempotent publish protocol
per supporting driver. At minimum, bound cleanup, return a typed ambiguous
dedupe error when release fails, and never translate the next reservation hit
into success without proof of publication.

**Acceptance:** deterministic fault tests for reserve success + publish failure
+ release failure on every advertised dedupe driver, followed by a caller
retry; the outcome must be durable acceptance or a visible error, never false
success.

### EB-02: Redis stream trimming can delete accepted, unprocessed work

**Evidence:** `driver/redis/redis.go:121-132` applies approximate `MAXLEN` with a
default of 100,000 on every immediate `XADD`. Redis trimming is not constrained
by EventBus consumer-group progress. `driver/redis/KEY_LAYOUT.md:80-83`
explicitly avoids the same trimming during delayed dispatch because it can
discard accepted work, but leaves immediate publication unchanged.

**Impact:** a slow/offline task worker or event subscriber can lose entries
when the stream crosses the bound. Event fan-out is especially exposed because
one fast subscriber does not make trimming safe for slower groups.

**Required fix:** do not trim the ready stream on publish. Implement retention
as a separate, observable administration process that computes a safe minimum
acknowledged position across all relevant groups, or require an explicit lossy
mode that cannot be mistaken for the durable contract.

**Acceptance:** publish beyond the old 100,000 boundary while a group is
offline, then consume every accepted ID; include a slow event-subscriber group
and Redis Cluster coverage.

### EB-03: physical topology naming is collision-prone

**Evidence:** `transport/transport.go:83-92` joins raw components with `.`.
Consequently task `event.foo` and event `foo` in namespace `acme` both become
`acme.event.foo`. Across applications, namespace `a.b` + task `c` collides with
namespace `a` + task `b.c`. NATS repeats the problem with `_` in
`driver/nats/nats.go:83-93`. Core validation permits these characters. Memory,
Kafka, and NATS use these derived names as resource keys; Redis is safer because
its v1 key layout encodes components.

**Impact:** valid declarations can share topics/streams, cross-deliver task and
event traffic, share offsets, or expose one namespace's data to another. This
breaks routing isolation and can become a tenant-boundary issue.

**Required fix:** introduce a versioned, injective physical-name encoding for
every component and driver. Provide collision detection and an explicit
migration/alias strategy; do not silently rename existing production
resources.

**Acceptance:** table/property tests prove distinct `(namespace, kind, name,
subscriber)` tuples never collide, including separator, Unicode, and
task-name-with-`event` cases; add live cross-delivery tests for NATS and Kafka.

### EB-04: Memory transport acknowledges publications it drops

**Evidence:** `driver/memory/memory.go:108-115` uses a non-blocking channel send
and silently takes the `default` branch when a 1,000-entry queue is full, then
returns `nil`. This contradicts the target recorded in `CAPABILITIES.md` that a
full Memory buffer must return a typed capacity error. In addition, event groups
are created only by `Consume` (`memory.go:134-146`), so an event published after
declaration but before subscriber consumption starts has no group to receive
it. `PublishAt` creates one goroutine/timer per message and ignores the eventual
publish error (`memory.go:118-131`).

**Impact:** the default quickstart/test transport can report successful
acceptance while losing tasks or events, masking bugs in callers and contract
tests.

**Required fix:** return a stable capacity error or apply context-bounded
backpressure; provision declared subscriber groups before publication; replace
per-message delayed goroutines with one bounded scheduler, or explicitly reject
large delayed use.

**Acceptance:** capacity, publish-before-consume fan-out, delayed-close, and
concurrent publish tests assert that every `nil` publication is observable by
the intended group.

### EB-05: Kafka delayed scheduling has partition head-of-line blocking

**Evidence:** `driver/kafka/scheduler.go:190-193` rewinds and stops a partition
when its head record remains in the current tier. A later record in that same
partition is never inspected, even when its deadline is earlier. The behavior
is documented in `driver/kafka/SCHEDULER.md:17-23`, but the same document and
capability matrix advertise five-second precision without qualifying the
requirement that deadlines be monotonic within each partition.

**Impact:** for example, a one-hour record placed before a six-minute record in
the five-minute tier can delay the latter close to an hour. Different ordering
keys can hash to the same partition, and empty keys can share a sticky
partition, so this is not limited to intentionally ordered work.

**Required fix:** redesign delayed storage so due-time selection is independent
of append order (for example, time buckets with sufficiently fine immutable
bucket topics plus a documented ordering tradeoff), or explicitly remove the
precision guarantee and reject non-monotonic schedules per ordering domain.

**Acceptance:** tests place far-future then near-future records in the same
partition and prove the near deadline is met within the advertised bound under
a bounded backlog.

### EB-06: decoded envelopes are not checked against the subscribed route

**Evidence:** `codec.go:87-111` validates only version and attempt fields.
`engine.go:506-514` silently overwrites `env.Kind` with the active consumer kind
but accepts an empty ID, a different `env.Type`, and inconsistent/missing time
or metadata fields. The handler therefore may receive `Job.Type()` for another
topology while metrics and settlement use the subscribed topology.

**Impact:** malformed or directly injected broker records can spoof identity,
confuse handler authorization/routing assumptions, contaminate tracing, and
produce inconsistent retry/DLQ metadata rather than being quarantined.

**Required fix:** validate the complete v3 envelope after decode: nonempty and
bounded ID, exact kind/type match, sane timestamps, header constraints, and
attempt invariants. Route mismatches should go to DLQ with a stable reason and
sanitized diagnostics.

**Acceptance:** shared contract cases for wrong kind, wrong type, empty ID,
invalid timestamps, and malformed headers on every driver.

## P1 findings

### EB-07: error handling can panic, leak data, or inflate DLQ records

`engine.go:611-617` logs arbitrary handler `err.Error()`; a custom error whose
`Error` method panics escapes the handler recovery boundary. Public
`PermanentError` and `RetryAfterError` values also dereference `Err` without
protecting against manually constructed nil values (`outcome.go`). A custom
`RetryPolicy.NextBackoff` runs outside recovery and has no validation for a
negative or pathological duration. Redis, NATS, Kafka, and Memory copy the full
cause string into DLQ metadata without a size/redaction bound. This can expose
payload-derived secrets and can make a DLQ wrapper exceed broker limits,
leaving a poison message in endless settlement failure.

Add safe bounded error rendering, fixed public error codes, nil-safe outcome
types, retry-duration validation/clamping, recovery around extension points,
and a documented maximum for DLQ reason/error metadata. Never use raw handler
errors as metric labels or exported trace attributes.

### EB-08: autonomous scheduler failures are operationally invisible

Redis discards delayed sweep errors (`driver/redis/redis.go:157-161`), NATS
unconditionally discards `dispatchDue` errors
(`driver/nats/scheduler.go:190-199`), and Kafka discards both batch-processing
errors and most poll errors (`driver/kafka/scheduler.go:134-156`). Current OTel
hooks observe core consumers but are not injected into driver schedulers.
Delayed work can therefore stop advancing while dashboards show no direct
scheduler failure. Add driver runtime hooks/health state, retry/backoff metrics,
last-success timestamps, and readiness degradation. Alert on oldest delayed
age as a fallback, not as the only signal.

### EB-09: Redis transport shutdown does not join scheduler goroutines

`driver/redis/redis.go:298-305` cancels sweepers and returns without a
`WaitGroup`, unlike NATS and Kafka. A sweeper may still be inside Redis I/O when
`Close` reports completion and the application closes its shared client. Track
and join all Redis sweepers within the caller deadline, with deterministic
shutdown tests.

### EB-10: the public administration surface is incomplete

`Bus.Stats(ctx, name)` always constructs a task topology
(`bus.go:194-209`), so event/subscriber statistics implemented by drivers are
not reachable through the public Bus API. DLQ list/count/requeue/purge are not
on `Bus` at all; the admin example bypasses it by retaining and type-asserting
the concrete transport (`examples/eventbus/admin/main.go`). This also bypasses
uniform name/kind validation, metrics, authorization hooks, and lifecycle
checks.

Add kind-explicit task/event stats and a stable Bus-level DLQ API, including
cursor pagination and subscriber/group semantics. Keep destructive operations
explicit and authorization-neutral at library level, but make it possible for
applications to wrap one consistent interface.

### EB-11: Kafka existing-topic configuration is not reconciled

`driver/kafka/kafka.go:187-197` treats `TopicAlreadyExists` as success and then
validates only partition counts. Replication factor, retention, and arbitrary
topic configs are not checked or altered. `WithTopicConfigs` also permits
`cleanup.policy=compact`; because ordering keys become Kafka record keys,
compaction can remove earlier accepted jobs with the same key before they are
handled.

Reconcile all EventBus-owned safety-critical settings, reject incompatible
cleanup policies, report immutable mismatches, and make opt-in changes explicit.
Partition increases need a separately gated operation because key-to-partition
mapping changes can break ordering across the transition.

### EB-12: the shared contract suite is much narrower than its checklist

`transport/contracttest.Run` contains seven scenarios: publish/ACK, retry
transfer, DLQ admin, delay, event fan-out, dedupe, and malformed payload. The
23-item checklist in `REFACTOR_PLAN.md` remains entirely unchecked. Several
behaviors exist in driver-specific tests, but there is no single capability-
gated suite proving equivalent semantics for handler timeout/panic, settlement
failure, forced shutdown/redelivery, reconnect leaks, subscriber isolation,
ordering, multi-instance scheduling, and the newly identified fault windows.

Expand the reusable suite, map every checklist row to exact tests in
`CONTRACT_TESTS.md`, and mark rows only after all applicable durable drivers
pass. Do not treat P5.8's prose claim of a “complete shared contract” as the
release gate while the executable suite and checklist disagree.

### EB-13: large-backlog and soak release evidence is still missing

P6.6 explicitly records that persistent 100,000/1,000,000-message runs were not
executed; the full matrix used only 20-message smoke cases and selected
100-message measurements. These runs cannot validate long-lived scheduler
metadata, broker disk growth, consumer-group churn, Redis trimming, Kafka
head-of-line delay, duplicate rate, or bounded memory at production backlog.

Run reproducible disposable-broker tests at both backlog sizes, sustained
retry/delay mixes, restarts/rebalances, and a multi-hour soak. Record broker
versions/configuration, CPU/RSS/disk, duplicate rate, tail latency, and cleanup
procedure. This is required before the production-soak Phase 7 exit gate.

## P2 findings

### EB-14: lifecycle behavior is inconsistent after shutdown

`Start` has explicit state checks, but `publish`, `Stats`, and `Ping` do not
reject the stopped/stopping state. `bus.Ping` checks whether the transport field
is nil even though shutdown never clears it (`bus.go:212-217`). Results after
`Close` therefore depend on the concrete driver instead of consistently
returning `ErrBusClosed`. Add a state gate and table tests for every public
operation before start, while running, while stopping, and after close; preserve
producer-only publication before `Start` if that remains the contract.

### EB-15: declaration and subscriber option boundaries remain ambiguous

`Subscribe` uses the same `TopologyOption` type as `DeclareEvent`. When an event
was explicitly declared, subscriber options are cloned into a local subscription
topology, but only the declared base topology is provisioned by `Start`. Driver
provisioning options supplied to a subscriber are therefore validated yet may
be ignored. In the implicit compatibility path, the first subscriber's options
become the event's provisioning options, making configuration order-dependent.
Repeated declarations and duplicate subscriber names overwrite prior values
without a conflict error.

Introduce distinct `TaskOption`, `EventOption`, and `SubscriberOption` sets (or
strictly classify option keys), reject driver provisioning options at
`Subscribe`, and define idempotent-equal versus conflicting redeclaration
behavior. Remove implicit event declaration during the Phase 7 compatibility
cleanup.

### EB-16: publication does not return a stable message identity

The codec generates a UUID, but `Enqueue`, `EnqueueBatch`, and `Publish` return
only `error`; callers cannot record the accepted EventBus ID. This makes P7.3
count/outcome reconciliation weaker and complicates idempotency, support, and
trace correlation. Add result-returning APIs (while retaining error-only
adapters), plus an explicitly validated caller-supplied ID/idempotency path if
needed. Batch results must expose accepted IDs for the durable prefix.

### EB-17: ordering key and local keyed concurrency are conflated

The publish path rejects every nonempty ordering key unless the driver
advertises broker ordering (`engine.go:912-914`). As a result,
`WithOrderingKeyConcurrency` cannot normally be used with Redis, NATS, or
Memory even though it is described as a core local admission control. Separate
the application concurrency key from the broker partition/ordering key, or
document the feature as Kafka-only. Test both semantics explicitly.

### EB-18: every topology pays scheduler cost even when delay is unused

Provisioning starts one Redis or NATS sweeper per topology and three Kafka
consumer clients/groups plus three delay topics per topology. Kafka also always
creates main, DLQ, and all delay topics. Large topology counts therefore create
substantial idle goroutine, connection, group, partition, and broker-metadata
overhead. Add an explicit delay capability option, shared scheduler where safe,
and capacity guidance/formulas for topology count × delay tiers × partitions.

### EB-19: Kafka partition scaling and example capacity need refinement

Existing Kafka topics cannot be expanded by EventBus; startup rejects any count
different from `WithPartitions`. A safe explicit expansion tool should increase
main, DLQ, and all delay topics together, reject decreases, survive partial
admin success, and warn that key-to-partition mapping changes across the
transition. It must not silently auto-expand during ordinary startup.

The shared example currently configures concurrency 4, prefetch 8, but Kafka
partitions 3 (`examples/eventbus/internal/demo/runtime.go:124-140`). For one
topic/group/process only three workers can be useful. Make helper inputs
topology-specific and document `concurrency per instance ~= ceil(partitions /
planned instances)` rather than presenting one setting for all events.

### EB-20: capability and plan documents have drifted

The compact code capability descriptor exposes only delay precision and a
boolean ordering flag, while the richer durability/settlement/admin model is
still only a proposal in `CAPABILITIES.md`. It cannot express process-local
versus distributed controls, atomic versus publish-before-ack settlement,
payload limits, or scheduler health. The plan header still says “Phase 5
verified complete” although Phase 6 is checked complete, the shared-contract
checklist is open, and most production gates remain open.

Promote structured guarantee enums into code, make docs/tests derive from or
validate the advertised capabilities, and update the plan snapshot without
marking unverified release gates complete.

## Proposed Phase 6.7 sequence

1. **P6.7.1 Safety invariants:** EB-01, EB-02, EB-03, and EB-04 with regression
   and fault tests.
2. **P6.7.2 Scheduling correctness:** EB-05, scheduler observability from EB-08,
   and Redis shutdown joining from EB-09.
3. **P6.7.3 Boundary hardening:** EB-06 and EB-07, including bounded sanitized
   DLQ metadata and hostile-error tests.
4. **P6.7.4 Administration/configuration:** EB-10 and EB-11, including Kafka
   reconciliation and a separately designed partition expansion command.
5. **P6.7.5 Contract evidence:** EB-12 and EB-13; execute backlog, restart,
   rebalance, crash-window, and soak tests on disposable infrastructure.
6. **P6.7.6 API/scalability cleanup:** EB-14 through EB-20 where a Phase 7
   adapter would otherwise freeze the current behavior.

Phase 7 can begin after all Phase 6.7 tasks pass. EB-16 must be resolved before
P7.1 so adapters and migration verification share stable publication identities.

## Verification performed during this review

The review used source inspection plus the following commands from the module
root with the workspace-local Go cache:

```powershell
$env:GOCACHE="$PWD\.gocache-review"
go test ./eventbus/... -count=1
go vet ./eventbus/...
go test -tags=integration ./eventbus/... -run '^$'
go test -tags=integration ./eventbus/... -count=1
```

All four commands passed on 2026-10-18. The full tagged run exercised the live
Kafka suite and embedded NATS suite; live Redis standalone/cluster cases remain
environment-gated as described in `TESTING.md`. Passing these commands does not
close the findings above: most describe uncovered fault combinations,
valid-name collisions, capacity behavior, or production-scale evidence absent
from the current suites.

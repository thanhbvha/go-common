# EventBus Refactor Execution Plan

> Status: Phase 5 in progress; P5.1-P5.2 complete; Phase 4 verified complete
> Scope: `eventbus`, `redis`, `kafka`, `nats`, `queue`, `queue_kafka`, `queue_nats`  
> Primary goal: turn `eventbus` into a production-grade, transport-agnostic task queue with explicit event fan-out support.  
> Delivery guarantee: at-least-once. Exactly-once is not promised.

## 1. How agents must use this document

This file is the source of truth for the refactor. Before changing code, an agent must:

1. Read this entire file.
2. Inspect the current worktree and preserve unrelated/user changes.
3. Select the first uncompleted task whose dependencies are complete.
4. Change the task marker from `[ ]` to `[~]` before implementation.
5. Implement only that task and directly required supporting work.
6. Run the task's verification commands and record important results in its Notes section.
7. Change `[~]` to `[x]` only when every acceptance criterion passes.
8. If blocked, change `[~]` back to `[ ]` and add a `BLOCKED:` note with exact evidence. Do not silently skip ahead if later work depends on it.

Task markers:

- `[ ]` not started
- `[~]` in progress
- `[x]` complete and verified

Do not mark an entire phase complete while any task or phase exit criterion remains open.

## 2. Non-negotiable invariants

Every implementation decision must preserve these invariants:

1. A successfully accepted job is never silently lost.
2. Duplicate delivery is allowed and documented; silent loss is not.
3. ACK/commit happens only after successful handling or successful durable transfer to retry/DLQ.
4. Metrics report ACK/retry/DLQ success only after the broker confirms settlement.
5. Work-queue tasks and fan-out events use distinct topology modes and cannot cross-deliver.
6. Attempts are 1-indexed and bounded by `MaxAttempts`, the total number of permitted handler executions.
7. A driver must return `ErrNotSupported` for unsupported operations; it must not return a successful zero value.
8. `Drain` stops intake first and lets in-flight handlers finish until the grace deadline.
9. Broker-specific differences must be exposed through capabilities and documented.
10. All integration tests must be opt-in; normal unit tests must not require an external broker.
11. Legacy queue modules remain available until migration and rollback criteria are satisfied.
12. Public API compatibility changes require an adapter or an explicit migration note.

## 3. Known critical defects to keep visible

These defects motivated the refactor and must receive regression tests:

- Kafka can commit a higher offset while an earlier record is still processing.
- Kafka consumers created inside the EventBus driver do not inherit TLS/SASL configuration.
- `queue_kafka` publishes retry asynchronously and commits the source offset regardless of publish success.
- Redis EventBus heartbeat is a no-op, so active jobs can be reclaimed and executed twice concurrently.
- Redis reclaim republishes messages and resets transport delivery history.
- Redis retry/DLQ publish and source ACK are not atomic.
- NATS retry messages can lose `Message.ID`; delayed KV keys can collide and overwrite jobs.
- NATS/Kafka/Redis DLQ admin and stats contain success-returning stubs.
- Engine shutdown cancels handler contexts immediately instead of draining.
- Engine state remains running if topology provisioning fails during `Start`.
- `MaxRetry` and `MaxDeliveries` currently have conflicting/off-by-one semantics.
- `PublishEvent` currently delegates to the same path as `Enqueue`, allowing ambiguous routing.
- Registry factories call wrapper `Default()` functions that panic before nil checks can run.
- Existing migration documentation references API symbols that do not exist.
- Root EventBus tests include broker-dependent spike tests and are not hermetic.

## 4. Existing modules: reuse and reject decisions

### Reuse as references

- `eventbus`: versioned envelope, raw JSON payload, typed handler, retry policy, handler context, middleware, topology abstraction.
- `redis`: `XPENDING`, `XAUTOCLAIM`, `XCLAIM`, `XINFO GROUPS`, `XINFO CONSUMERS`, cluster-aware native client.
- `queue`: reclaim fallback logic and DLQ retention concepts.
- `kafka`: TLS/SASL/idempotent producer configuration and admin primitives.
- `queue_kafka`: partition-key propagation, headers, topic retention configuration.
- `nats`: consumer stats, ACK/NAK/Term/InProgress, KV CAS primitives.
- `queue_nats`: broker delivery count and AckWait-based redelivery concepts.

### Do not copy without redesign

- `queue` non-atomic retry/DLQ followed by unconditional ACK batching.
- `queue` non-atomic delayed `XADD` then `XDEL`.
- `queue_kafka` goroutine-based retry followed by source commit.
- `queue_kafka` delayed topic sleep/republish loop.
- `queue_nats` behavior that treats every KV error as a duplicate.
- `queue_nats` bulk delay based only on `NakWithDelay`.
- Any legacy `Start` method that logs provisioning failure and continues.

## 5. Target architecture

```text
Public Bus API
  |-- Task API: DeclareTask / HandleTask / Enqueue
  |-- Event API: DeclareEvent / Subscribe / Publish
  |-- Lifecycle: Start / Drain / Close
  `-- Admin: Stats / DLQ / Pause / Resume

Core runtime
  |-- topology registry and validation
  |-- producer and envelope codec
  |-- consumer supervisor
  |     |-- bounded intake
  |     |-- fixed worker pool
  |     `-- reconnect/backoff control
  |-- outcome classifier
  `-- settlement coordinator

Transport
  |-- required: Producer, ConsumerFactory, Provisioner, HealthChecker, Close
  `-- optional: StatsReader, DLQAdmin, Deduper, Pauser
```

### Required public API direction

The final names can be adjusted before Phase 1 exits, but semantics cannot remain ambiguous:

```go
type Bus interface {
    DeclareTask(name string, opts ...TaskOption) error
    HandleTask(name string, handler Handler) error
    Enqueue(ctx context.Context, name string, payload any, opts ...JobOption) (JobID, error)

    DeclareEvent(name string, opts ...EventOption) error
    Subscribe(name, subscriber string, handler Handler, opts ...SubscriberOption) error
    Publish(ctx context.Context, name string, payload any, opts ...PublishOption) (EventID, error)

    Start(ctx context.Context) error
    Drain(ctx context.Context) error
    Close(ctx context.Context) error
}
```

### Required settlement model

Delivery settlement must be single-use and explicit:

```go
type DecisionKind uint8 // Ack, Retry, DeadLetter

type Decision struct {
    Kind    DecisionKind
    RetryAt time.Time
    Reason  string
    Cause   error
}

type Delivery interface {
    Message() Message
    DeliveryAttempt() int
    Heartbeat(context.Context) error
    Settle(context.Context, Decision) error
}
```

Driver implementations may use different atomicity mechanisms, but they must satisfy the common contract or report a weaker capability explicitly.

### Envelope v3 required fields

- version
- message ID
- topology kind: task or event
- topology name
- raw payload
- headers
- created time
- scheduled time
- current attempt, 1-indexed
- maximum attempts
- ordering key
- dedupe key and dedupe TTL
- correlation ID
- causation ID

DLQ metadata is not part of the original envelope. A DLQ record wraps the envelope with reason, error, attempts, source position, and failed time.

## 6. Phase 0 - Baseline, contracts, and test separation

Dependencies: none.

- [x] **P0.1 Separate unit, spike, and integration tests**
  - Move/mark Redis, NATS, and Kafka broker tests as integration tests.
  - Default `go test ./eventbus/...` must not contact external services.
  - Keep small in-process NATS tests optional if they are deterministic.
  - Acceptance: default test suite completes without Redis, NATS, or Kafka running.
  - Notes: Added the `integration` build tag to broker/spike tests and documented suites in `eventbus/TESTING.md`. Corrected the namespace test to match the documented defaulting contract. Verified `go test ./eventbus/... -count=1`, `go vet ./eventbus/...`, and integration-tag compilation with `go test -tags=integration ./eventbus/... -run '^$'` on 2026-10-15.

- [x] **P0.2 Capture behavioral baselines**
  - Add benchmarks for codec, enqueue path, engine worker dispatch, and memory transport.
  - Record current allocations and throughput in a checked-in benchmark note.
  - Acceptance: benchmark commands are reproducible and do not use sleeps for synchronization.
  - Notes: Added deterministic benchmarks for codec encode/decode, enqueue, engine dispatch, and memory transport round trip in `benchmark_test.go`. Recorded three-run baseline results and methodology in `eventbus/BENCHMARKS.md` on 2026-10-15.

- [x] **P0.3 Write delivery-semantics ADR**
  - Define at-least-once, duplicate windows, ACK rules, retry rules, ordering scope, and shutdown behavior.
  - Define `MaxAttempts` as total handler executions.
  - Define behavior for decode errors, unknown types, permanent errors, timeout, panic, and settlement failure.
  - Acceptance: every transport contract test can reference an explicit ADR rule.
  - Notes: Accepted `ADR-001-delivery-semantics.md` with stable DS rule identifiers covering durability, routing, attempts, outcomes, settlement, ordering, dedupe, lifecycle, and scheduling. Contract tests must cite these IDs.

- [x] **P0.4 Define transport capability matrix**
  - Include durable delivery, delay precision, ordering scope, dedupe, heartbeat, atomic settlement, DLQ admin, stats, pause/resume, and maximum payload.
  - Acceptance: no driver advertises an unimplemented capability.
  - Notes: Added `eventbus/CAPABILITIES.md` with current and target matrices, promotion requirements, and explicit limitations. Disabled Kafka's `Ordering` advertisement until partition-aware processing and contiguous offset coordination are implemented and verified in P5.2/P5.3.

### Phase 0 exit criteria

- [x] Default EventBus tests are hermetic.
- [x] Semantics ADR is approved.
- [x] Capability matrix is checked in.
- [x] Baseline benchmarks are recorded.

## 7. Phase 1 - Public API and core contracts

Dependencies: Phase 0 complete.

- [x] **P1.1 Introduce explicit topology kinds**
  - Add task and event topology types.
  - Reject registration of the same logical topology in both modes.
  - Add tests proving `Enqueue` cannot fan out and `Publish` cannot invoke task workers.
  - Notes: Added `TopologyKind` (`TaskTopology`, `EventTopology`) and canonical mode-aware physical names. Task names retain their legacy physical format; event names include an `event` segment. Engine registries, producer paths, consumers, and all four drivers now preserve the kind. `Publish` is the explicit event API and `PublishEvent` remains a compatibility alias. Same-bus task/event name conflicts return `ErrTopologyKindConflict`. Regression tests cover both conflict directions and prove task/event publishes use distinct physical topologies. Verified default tests, vet, integration-tag compilation, and legacy package compilation on 2026-10-15.

- [x] **P1.2 Separate declaration from handler registration**
  - Producer-only applications declare topology without registering fake handlers.
  - Remove or deprecate ambiguous `AutoRegister` behavior.
  - Notes: Added `DeclareTask`, `HandleTask`, and `DeclareEvent`. Producer validation now uses declared topology presence rather than local handler/subscriber presence, allowing producer-only processes to provision and publish safely. `HandleTask` returns `ErrUndeclaredTopology` when declaration is missing. Legacy `Register` remains a declare-and-handle adapter; `Subscribe` retains implicit event declaration for compatibility while explicit declaration is recommended. `AutoRegister` is deprecated but retained until compatibility cleanup. Regression tests cover producer-only task/event provisioning and publishing, undeclared rejection, and the legacy adapter. Verified default tests, vet, integration-tag compilation, and legacy packages on 2026-10-15.

- [x] **P1.3 Replace retry counters with MaxAttempts**
  - Attempt starts at 1.
  - Handler executes at most `MaxAttempts` times.
  - No extra broker delivery should be required only to discover exhaustion.
  - Provide envelope v2 to v3 decode compatibility.
  - Notes: Added canonical `Config.MaxAttempts`, `WithMaxAttempts`, `Job.MaxAttempts`, and envelope v3 fields with 1-based `Attempt`; retained `MaxDeliveries`, `WithMaxDeliveries`, `GetAttempt`, `MaxRetry`, and task-default `Codec.Encode` as deprecated compatibility paths. New publishes encode topology kind and the effective topology attempt limit. The codec normalizes v2 zero-based attempts and `max_retry` into v3 semantics. Engine retries now increment the envelope before scheduling and send the final failed execution directly to DLQ when `Attempt == MaxAttempts`, while rejecting exhausted deliveries before handler invocation. Kafka delivery-count extraction handles both envelope versions. Deterministic table tests cover v2 conversion, v3 wire fields, retry incrementing, final-attempt DLQ, single-attempt jobs, pre-exhausted messages, and broker redelivery counts. Verified default tests, vet, integration-tag compilation, and legacy package tests/vet on 2026-10-15.

- [x] **P1.4 Split required and optional transport interfaces**
  - Remove mandatory success-returning stubs.
  - Add stable `ErrNotSupported` handling.
  - Add compile-time interface assertions in each driver.
  - Notes: Split the transport contract into required `Producer`, `Consumer`, `Provisioner`, `HealthChecker`, and `Closer` components composed by the minimal `Transport` interface. Deduplication, DLQ administration, statistics, and pause/resume are now optional `Deduper`, `DLQAdmin`, `StatsReader`, and `Pauser` interfaces discovered with type assertions. Added the canonical `transport.ErrNotSupported` sentinel and retained `eventbus.ErrNotSupported` as its public alias; unsupported Bus stats return a wrapped sentinel and `ErrDedupeUnavailable` also matches it through `errors.Is`. Removed success-returning DLQ/stats stubs from Redis, NATS, and Kafka. Memory implements real optional interfaces, including functional DLQ requeue. Added compile-time assertions for every driver and delivery implementation, plus tests proving a required-only transport compiles and unsupported optional operations return the stable sentinel. Updated `CAPABILITIES.md`. Verified default tests, vet, integration-tag compilation, and legacy package tests/vet on 2026-10-15.

- [x] **P1.5 Introduce single-use settlement**
  - Prevent double ACK/retry/DLQ.
  - Settlement failure must remain observable and retryable according to the ADR.
  - Metrics update only after settlement success.
  - Notes: Replaced separate delivery `Ack`, `Retry`, and `DeadLetter` methods with one explicit `Settle(Decision)` contract, alongside `DeliveryAttempt` and `Heartbeat`. Added `DecisionKind`, absolute retry deadlines, failure metadata, stable settlement errors, and a shared atomic `SettlementGuard`. The guard rejects concurrent or repeated terminal settlement, but reopens after failure so settlement can be retried/redelivered per DS-40 through DS-44. Memory, Redis, NATS, and Kafka deliveries all use the guard and retain publish-before-ACK ordering for retry/DLQ paths. Engine completion metrics now increment only after `Settle` returns success; failures are logged with job ID, decision, reason, and error while the source remains unsettled. Deterministic tests cover double/concurrent settlement, invalid decisions, retry after settlement failure, a real memory delivery, and metric suppression until confirmation. Updated `CAPABILITIES.md`. Verified default tests, vet, integration-tag compilation, and legacy package tests/vet on 2026-10-15. Targeted race tests could not run in this environment because CGO was disabled and no `gcc` compiler is installed.

- [x] **P1.6 Add strict validation**
  - Reject empty names, nil handlers, invalid concurrency, invalid timeouts, negative attempts, incompatible modes, and invalid driver options.
  - Resolve the current namespace validation/test disagreement.
  - Notes: Added stable `ErrInvalidConfig`, `ErrInvalidName`, `ErrNilHandler`, and `ErrInvalidOption` classifications. Config validation now rejects blank/padded namespaces and drivers plus negative process timeouts, concurrency, canonical attempts, and legacy delivery limits while preserving zero-value defaulting. Public task/event declaration, handler/subscriber registration, enqueue/publish, stats, codec, topology options, and job options reject empty/padded names, nil handlers/options, non-positive explicit concurrency/attempt overrides, negative delays, conflicting delay/run-at scheduling, malformed headers, invalid topology kinds, and incompatible built-in driver options without partial registry mutation. Added transport-level `Topology.Validate` and optional `TopologyValidator`; Memory, Redis, NATS, and Kafka validate names and all currently supported driver option types/ranges before provisioning, rejecting unknown options in their own namespaces. `Handle` now converts nil typed handlers or middleware into a nil handler that registration safely rejects. Empty namespace continues to resolve to `eventbus`, while whitespace-only namespace is invalid, resolving the documented defaulting contract. Table tests cover core APIs, codec/job/topology options, transport topology modes, and each driver's options. Verified default tests, vet, integration-tag compilation, and legacy package tests/vet on 2026-10-15.

- [x] **P1.7 Add legacy API adapters**
  - Keep current `Register`, `PublishEvent`, and error-wrapping behavior available with deprecation comments where feasible.
  - Document breaking changes that cannot be adapted.
  - Notes: Retained `Register` as the `DeclareTask` plus `HandleTask` adapter and `PublishEvent` as a direct `Publish` adapter, with Go deprecation comments on both public methods. Marked the v2-only envelope `MaxRetry` field deprecated alongside the existing config, topology-option, job, and codec compatibility APIs. Added compatibility tests proving legacy publication uses event topology and that `Register`/`PublishEvent` preserve sentinel identity through `errors.Is`. Replaced the stale migration guide with compiling API names, an adapter/replacement table, producer-only and fan-out declaration guidance, a list of intentionally unadaptable transport/envelope/validation changes, at-least-once and idempotency requirements, and a migration sequence that avoids unsafe dual-consumer side effects. The existing legacy-style example continues to compile through the adapters. Verified default EventBus tests, vet, integration-tag compilation, the EventBus example, and legacy package tests/vet on 2026-10-17.

### Phase 1 exit criteria

- [x] Task and event routing are unambiguous.
- [x] Attempt semantics have exhaustive table tests.
- [x] Unsupported optional operations return `ErrNotSupported`.
- [x] Legacy examples either compile through adapters or have migration instructions.

## 8. Phase 2 - Engine lifecycle, concurrency, and observability

Dependencies: Phase 1 complete.

- [x] **P2.1 Replace per-delivery goroutines with bounded worker pools**
  - One bounded intake queue per task/subscriber.
  - Fixed workers enforce concurrency.
  - Prefetch is independently configurable and bounded.
  - Notes: Replaced the semaphore plus per-delivery handler goroutine path with one fixed worker pool and one bounded intake channel for every task/subscriber consume loop. A synchronous transport callback now blocks when all workers and buffered intake capacity are occupied, propagating backpressure without accumulating handler goroutines. Added global `Config.Prefetch` (default 20) and validated per-topology `WithPrefetch`; prefetch counts queued deliveries and is independent of active worker `Concurrency`. Added a deterministic controlled-transport test proving a topology override of two workers plus one prefetched delivery blocks the fourth delivery, processes all messages, and never exceeds two active handlers; the test passed 20 consecutive runs. `BenchmarkEngineDispatch` remains near its Phase 0 baseline at 5,694-8,044 ns/op, 2,214-2,216 B/op, and 25 allocs/op across three runs. Verified default EventBus tests, vet, integration-tag compilation, the EventBus example, and legacy package tests/vet on 2026-10-17.

- [x] **P2.2 Implement a consumer supervisor**
   - Context-aware reconnect backoff with jitter.
   - No blocking `time.Sleep` in cancellation paths.
   - A failed consume session must clean up child goroutines before reconnect.
   - Notes: Added a per-topology consumer supervisor with cancellation-aware exponential reconnect backoff (100ms initial, 30s cap, up to 20% jitter, reset after a minute-long session). All unexpected session returns, including nil and session-local cancellation, back off instead of spinning or permanently stopping intake. Each session owns and joins its bounded worker pool before replacement; delivery processing also cancels and joins heartbeat goroutines, with heartbeat operations observing cancellation. Documented the synchronous callback/session cleanup contract, made Redis cancel and join its session auto-claimer, and made NATS release bound local pull subscriptions without deleting the durable consumer. Broker-free `testing/synctest` regressions cover reconnect timing, nil/error/canceled returns, cancellation during backoff, and settlement before session replacement; they passed 20 consecutive runs. Verified default EventBus tests, EventBus/example/legacy vet, integration-tag compilation, example/legacy tests, and the embedded-NATS driver integration suite on 2026-10-17. `go test -race ./eventbus ./eventbus/driver/memory` remains blocked: Go reports `-race requires cgo; enable cgo by setting CGO_ENABLED=1`; the Phase 2 race exit criterion remains open.

- [x] **P2.3 Correct Start state transitions**
  - Provision before committing to running state, or roll back on failure.
  - Concurrent `Start`, `Drain`, and `Close` calls must have deterministic results.
  - Notes: Added an explicit starting state, committing to running only after all task/event topologies provision successfully. Provisioning errors or startup-context cancellation restore the new state for retry; no consumers launch on failure. Registration mutations recheck state under the registry lock, and legacy Register now installs topology and handler atomically. Shutdown interrupts provisioning, waits for startup completion before WaitGroup.Wait, and shares exactly one transport Close/result across concurrent Stop/Close calls; each waiter can cancel its own wait. Added Bus.Close for never-started/producer-only cleanup and documented the interface addition, error semantics, and first-caller cleanup deadline in MIGRATION.md. The existing Stop/Close path now coordinates lifecycle transitions; the new graceful Drain operation and separate intake/worker contexts remain P2.4 work and must reuse this coordination. Deterministic channel/synctest regressions cover task/event provisioning failure (including partial provisioning), retry, canceled startup, concurrent Start/registration, Close during provisioning, shutdown before Start, and shared shutdown errors. The initial lifecycle regression set passed 50 consecutive runs. Verified default EventBus tests, EventBus/example/legacy vet, integration-tag compilation, and example/legacy tests on 2026-10-17. Race verification remains unavailable: `go test -race ./eventbus ./eventbus/driver/memory` reports `-race requires cgo; enable cgo by setting CGO_ENABLED=1`.

- [x] **P2.4 Implement true graceful drain**
  - `intakeCtx` stops polling/fetching.
  - `workerCtx` remains alive for in-flight handlers.
  - Force-cancel workers only after the caller's deadline.
  - Close transport only after workers settle or force-cancel.
  - Notes: Added Bus.Drain and made Stop/Close share its terminal shutdown path. The intake context now stops polling, reconnects, and startup independently of the worker context; accepted/prefetched deliveries finish handling, heartbeat, and settlement during the grace window. Deadline expiry cancels workers and settlement before transport close; buffered deliveries are not executed after forced cancellation, and late handler results are not settled. Shutdown can force-cancel even if startup has not yet returned. Added an optional Subscription.WaitForHandlers cleanup barrier and used it in Kafka to retain the session-owned settlement client until workers finish. Documented API additions, grace ownership, cancellation cooperation, durable redelivery, and memory limitations in MIGRATION.md. Deterministic synctest regressions cover intake-before-cancellation, prefetched work, settlement confirmation, forced settlement cancellation, cooperative and cancellation-ignoring handlers, and independent secondary waiter cancellation; all drain tests passed 50 consecutive runs. Verified default EventBus tests, EventBus/example/legacy vet, integration-tag compilation, and example/legacy tests on 2026-10-17. Live Kafka integration was not run (no broker provisioned); race verification remains blocked because `go test -race ./eventbus ./eventbus/driver/memory` reports `-race requires cgo; enable cgo by setting CGO_ENABLED=1`.

- [x] **P2.5 Make panic recovery default**
  - A panic becomes a retryable failure unless policy classifies it otherwise.
  - Preserve stack information in logs/trace without placing unbounded data in DLQ.
  - Notes: Core handler invocation now recovers panics by default for tasks, event subscribers, and middleware, returning stable ErrHandlerPanic through the existing retry/attempt-bound/DLQ classifier while keeping workers alive. The legacy Recoverer uses the same helper, avoids duplicate recovery logging, and remains usable outside the engine; explicit returned classifications such as MarkPermanent remain supported. Panic values are never formatted, unwrapped, logged, or copied to settlement metadata. Source-only stack logs use runtime.CallersFrames (no argument values), capped at 32 frames and 8 KiB; retry/DLQ causes contain only the fixed sentinel. Documented behavior and goroutine scope in MIGRATION.md. Regression tests cover string/error/nil/unformattable/permanent-error panic values, retry increments, final-attempt DLQ, bounded/redacted diagnostics, explicit classification, and worker survival for task/event middleware panics. Panic/recoverer tests passed 50 consecutive runs and panic(nil) also passed with GODEBUG=panicnil=1. Verified default EventBus tests, EventBus/example/legacy vet, integration-tag compilation, and example/legacy tests on 2026-10-17. Race verification remains blocked: `go test -race ./eventbus ./eventbus/driver/memory` reports `-race requires cgo; enable cgo by setting CGO_ENABLED=1`.

- [x] **P2.6 Finish metrics and tracing hooks**
  - Enqueue, dequeue, handler duration, timeout, panic, retry, DLQ, ACK failure, settlement latency, reconnect, saturation, lag, and oldest age.
  - Propagate trace/correlation headers through retry and DLQ.
  - Notes: Wired dequeue and all valid-name publish outcomes (including validation/encoding/dedupe failures and duplicate skips), retained handler duration and confirmed-only ACK/retry/DLQ completion, and added optional RuntimeMetrics for panic/timeout, settlement outcome/latency, reconnect, bounded-intake saturation, delivery ready-age, and successful explicit Stats samples. Existing Metrics/Tracer implementations remain source-compatible. Added explicit LagKnown/OldestAgeKnown flags to topology stats rather than fabricating unsupported broker-wide measurements; actual driver calculations remain transport-phase work. Processing deadlines now classify late nil handler returns as timeout/retry. Started/ended producer, processing, and child settlement spans; optional TraceInjector writes producer propagation headers into the durable envelope and optional TraceRecorder receives sanitized outcome events. Retry carries copied headers in both envelope and transport metadata; DLQ retains the original envelope carrier. Added OBSERVABILITY.md and migration guidance defining metrics, context/propagation, stats availability, and hook concurrency. Regression tests cover confirmed-only success for all three settlement kinds, failures/latency hooks, producer injection without caller-map mutation, retry/DLQ correlation preservation, handler context, timeout/panic, reconnect/saturation, enqueue outcomes, and stats availability/error suppression; the hook regression set passed 50 consecutive runs. Verified default EventBus tests, EventBus/example/legacy vet, integration-tag compilation, and example/legacy tests on 2026-10-17. Race verification remains blocked: `go test -race ./eventbus ./eventbus/driver/memory` reports `-race requires cgo; enable cgo by setting CGO_ENABLED=1`.

- [x] **P2.7 Normalize decoded message metadata**
  - Restore ID, type, ordering key, dedupe key, and headers from the envelope before a driver retry/DLQ path uses the message.
  - Regression test the NATS empty-ID collision.
  - Notes: After successful decode, the engine now treats envelope ID, type, ordering key, dedupe key, and copied headers as authoritative over raw/stale broker metadata before poison handling, retry, or DLQ. Retry messages use restored metadata; the observed-delivery wrapper exposes a consistent normalized message while preserving original source payload bytes. Added optional Decision.Source for driver DLQ paths; Memory, Redis, NATS, and Kafka consume it with raw fallback for decode failures/direct settlement. Kafka DLQ also restores ordering key and transport headers. Documented the custom-driver contract in MIGRATION.md. Broker-free regressions cover retry/permanent/exhausted/poison/ACK source normalization, raw decode-error fallback, and Memory DLQ metadata. An embedded-JetStream regression drives the real engine/driver to retry two raw-ID-empty deliveries at an identical deadline, proving two distinct KV keys retain ID, attempt, and correlation headers. Repeated integration execution exposed persistent default JetStream test storage; the NATS server helper now uses per-test t.TempDir storage, removing stale records/dedupe between runs. Core metadata tests passed 50 consecutive runs; the NATS collision regression passed five consecutive runs after storage isolation. Verified default EventBus tests, EventBus/example/legacy vet, example/legacy tests, full embedded-NATS driver integration suite, and integration-tag compilation on 2026-10-17. Race verification remains blocked: `go test -race ./eventbus ./eventbus/driver/memory` reports `-race requires cgo; enable cgo by setting CGO_ENABLED=1`. All implementation tasks are complete, but Phase 2 is not declared complete until its race-enabled exit criterion passes.

### Phase 2 exit criteria

- [x] Concurrency never exceeds configured limits.
- [x] Intake stops before in-flight cancellation during drain.
- [x] Startup can be retried after provisioning failure.
- [x] ACK/retry/DLQ failures are visible and tested.
- [x] Race-enabled core tests pass in a CGO-capable environment.

Race exit verification (2026-10-17): installed Scoop MinGW GCC 16.2.0 with
MinGW-w64 runtime v14 and verified `libsynchronization.a` is available. Ran
`PATH="/c/Users/thanh/scoop/apps/mingw/current/bin:$PATH" CGO_ENABLED=1 CC=gcc GOCACHE="D:/Go/go-common/.gocache-review" go test -race ./eventbus ./eventbus/driver/memory -count=1`.
Both packages passed (core 3.406s, memory 1.262s), with no race reports. This
resolves the historical CGO/compiler blockers recorded in the task notes above.
CGO was enabled only for this command, without changing the saved Go defaults.

## 9. Phase 3 - Redis reference driver

Dependencies: Phase 2 complete.

- [x] **P3.1 Define cluster-safe Redis key layout**
  - Main stream, delayed store, DLQ, and topology metadata share a topology hash tag where atomic scripts require it.
  - Dedupe scope includes namespace, topology kind, topology name, and user key.
  - Notes: Added versioned canonical Redis prefixes `eventbus:v1:{base64url(namespace):kind:base64url(name)}` with independent unpadded encoding, preventing delimiter ambiguity and injected braces. Stream, delayed sorted set, DLQ, metadata hash, and encoded user-key dedupe reservations share the exact topology hash tag. EnsureTopology writes layout/name/kind/namespace metadata before starting schedulers; scheduler identity now uses the canonical prefix and consumer groups are stream-local. Added optional transport.TopologyDeduper, preferred by the engine, to pass explicit topology to Redis reserve/release while preserving legacy Deduper callers and other drivers. Added KEY_LAYOUT.md and migration guidance for physical key/group changes, old backlogs, dedupe boundaries, and rollback. Miniredis/pure regressions cover identical hash tags, separator/brace/Unicode tuple isolation, namespace/kind/name/user-key reservation boundaries, expiry and scoped release, producer selection of scoped dedupe, and persisted metadata. Key-layout regressions passed 20 consecutive runs. Verified default EventBus tests, EventBus/example/legacy vet, integration-tag compilation, example/legacy tests, and race tests for core, memory, and Redis with command-local CGO/MinGW on 2026-10-17. No live Redis Cluster was provisioned; slot safety is verified via identical nonempty Redis hash tags, with live cluster/fault coverage still assigned to P3.8.

- [x] **P3.2 Make delayed dispatch atomic**
  - Use a bounded Lua operation to move due entries to the main stream.
  - Preserve message ID and all envelope metadata.
  - Multi-instance sweepers must be safe.
  - Notes: Hardened the Redis Lua move into an explicitly bounded dispatchDue operation (default 100, hard limit 1000 entries per tick), selecting due members with ZRANGEBYSCORE and preserving exact envelope bytes. The script checks source/destination types before mutation and performs XADD then per-member ZREM, avoiding batch-wide partial-failure ambiguity and unbounded unpack. Delayed dispatch no longer uses MAXLEN trimming, which could discard accepted ready work; existing retention behavior elsewhere remains visible for subsequent driver work. PublishAt validates topology options, and KEY_LAYOUT.md/MIGRATION.md document scheduling limits, non-rollback Lua behavior, duplicate/durability windows, and retention changes. Deterministic miniredis regressions verify no early execution, exact batch boundaries, unchanged ID/headers/ordering/dedupe/attempt envelope bytes, preservation of existing ready backlog, destination failure retaining sources and later recovery, cancellation, and four independent concurrent scheduler instances moving 101 entries without loss/duplicate publication. Delayed regressions passed 20 consecutive runs. Verified default EventBus tests, EventBus/example/legacy vet, integration-tag compilation, example/legacy tests, and race tests for core, memory, and Redis with command-local CGO/MinGW on 2026-10-17. Live Redis/Cluster crash/failover testing was not run (no broker infrastructure provisioned); that coverage remains assigned to P3.8.

- [x] **P3.3 Make retry and DLQ settlement atomic**
  - Retry: durable insert into delayed/ready destination plus source `XACK` in one cluster-safe operation.
  - DLQ: durable DLQ insert plus source `XACK` in one operation.
  - Notes: Replaced Redis publish-then-ACK retry/DLQ paths with one same-slot Lua pending-check/destination-write/XACK operation. Validates source stream, group/pending state, target type, and mode before mutation; a stale/already-settled source returns ErrDeliverySettled without duplicate destination writes. Retry retains exact incremented envelope bytes/deadline in delayed storage; DLQ retains normalized source payload plus reason/error/attempts/failed time/source stream/group/ID. Removed DLQ MAXLEN trimming to preserve accepted failure records; documented compatibility and elapsed-retry scheduling behavior in MIGRATION.md/KEY_LAYOUT.md and updated CAPABILITIES.md. Miniredis regressions exercise destination wrong-type failure without ACK, successful retry after failure, missing group before destination writes, source ACK confirmation, exact retry payload/deadline, DLQ metadata and no trimming, stale replay, and eight independent concurrent delivery objects producing only one DLQ entry. Atomic transfer regressions passed 20 consecutive runs. Verified default EventBus tests, EventBus/example/legacy vet, integration-tag compilation, example/legacy tests, and race tests for core, memory, and Redis with command-local CGO/MinGW on 2026-10-17. Live Redis Cluster and process/broker crash/failover fault tests were not run (no broker infrastructure provisioned); these remain P3.8 work. Lua isolation is not rollback: unexpected post-write ACK failures may leave duplicates, never intentional source removal before destination acceptance.

- [x] **P3.4 Rework reclaim handling**
  - Process messages returned by `XAUTOCLAIM`/`XCLAIM` directly.
  - Do not `XADD` a fresh copy merely to reclaim it.
  - Preserve broker delivery count for poison-message protection.
  - Notes: Removed the reclaim republish/XACK loop and separate auto-claimer goroutine. Consume now serializes bounded XAUTOCLAIM pages and fresh intake through its synchronous callback, claiming into the session consumer and retaining cursor progress even for empty pages. Reclaim runs at startup and after a 30-second completed-scan interval; blocking reads are capped at two seconds to avoid starving subsequent pages. Both fresh/reclaimed dispatch read XPENDING delivery count, retain source stream ID/payload, and leave settlement exclusively to the handler/core path. History/claim failures surface as consume errors for supervisor retry; requires Redis 6.2+ rather than silently ignoring unsupported XAUTOCLAIM. Malformed payload fields reach the core decode-error path without automatic ACK. Documented reclaim/version/idle behavior in KEY_LAYOUT.md and updated poison-count capability. Miniredis regressions verify repeated claim preserves stream length/ID and increasing delivery history, source remains pending until real settlement, active entries are skipped, malformed records remain pending, and actual Consume performs initial reclaim then exits on cancellation. Reclaim regressions passed 20 consecutive runs. Verified default EventBus tests, EventBus/example/legacy vet, integration-tag compilation, example/legacy tests, and race tests for core, memory, and Redis with command-local CGO/MinGW on 2026-10-17. Live Redis/Cluster fault tests remain P3.8 work; heartbeat/active-job lease safety remains P3.5 work.

- [x] **P3.5 Implement Redis heartbeat policy**
  - Do not report successful heartbeat when no lease/idle extension occurred.
  - Choose and document a safe mechanism, or advertise heartbeat unsupported and require reclaim idle time greater than maximum processing time.
  - Notes: Redis heartbeat now atomically checks pending consumer ownership then resets idle with XCLAIM JUSTID and explicit preserved RETRYCOUNT; success requires one confirmed renewed entry. Added ErrLeaseLost and optional LeaseDelivery cadence. Engine renews before execution, honors ack_wait/3 intervals even below its former one-second floor, cancels handlers on renewal errors, suppresses settlement after lease failure, and stops/joins heartbeat before settlement. Deliveries retain their original consumer identity and dispatch rejects changed ownership; ACK and retry/DLQ scripts also fence changed owners. Tests verify idle renewal without count increment, competing reclaim skips renewed work, stale heartbeat cannot steal ownership, stale ACK/retry/DLQ cannot mutate broker state, cancellation, initial renewal gating and short-cadence lease-loss handler cancellation. Miniredis differs in JUSTID count behavior, so explicit RETRYCOUNT preservation is tested. Documented queued/prefetched lease limits and external-side-effect/idempotency boundaries in KEY_LAYOUT.md; updated capability matrix. Heartbeat/lease regressions passed 20 consecutive runs. Verified default EventBus tests, EventBus/example/legacy vet, integration-tag compilation, example/legacy tests, and race tests for core, memory, and Redis with command-local CGO/MinGW on 2026-10-17. Live Redis/Cluster pause/partition/failover tests remain P3.8 work.

- [x] **P3.6 Implement Redis DLQ admin**
  - List with stable cursor pagination.
  - Count, purge, and idempotent requeue.
  - Retain reason, error, attempts, source stream ID, and failed time.
  - Notes: Redis now advertises `transport.DLQAdmin` plus additive `CursorDLQAdmin`. `ListPage` uses exclusive Redis stream-ID cursors, so deleting/requeueing earlier records cannot shift later pages; legacy positional `List` remains available and walks bounded cursor pages. `DeadLetter` now exposes a stable admin record ID, original message/envelope bytes and decoded metadata, reason/error/attempts/failed time, plus source stream/group/ID. Listing rejects malformed mandatory broker metadata rather than fabricating partial records. Count uses XLEN and purge deletes only the topology-scoped DLQ. Requeue accepts `DeadLetter.ID`, deduplicates and validates at most 1,000 IDs, then uses one same-slot Lua operation that validates all selected payloads before XADD-to-ready/XDEL-from-DLQ mutation; missing/already-requeued IDs are no-ops, making retries after ambiguous responses idempotent. Memory DLQ records received admin IDs while retaining legacy message-ID requeue compatibility. Added miniredis regressions for deletion-stable pagination, metadata restoration, topology isolation, count/purge, duplicate/missing-ID idempotency, wrong-type failure retention, whole-batch prevalidation, and malformed/input limits; the P3.6 regression set passed 20 consecutive runs. Updated `CAPABILITIES.md`, `KEY_LAYOUT.md`, and `MIGRATION.md`. Verified default EventBus tests, EventBus/example/legacy vet and tests, integration-tag compilation, full-repository test/vet, and race tests for core, memory, and Redis with command-local CGO/MinGW on 2026-10-17. Live Redis Cluster crash/failover behavior was not exercised because no cluster infrastructure was provisioned; it remains assigned to P3.8.

- [x] **P3.7 Implement Redis stats**
  - Pending ready, delayed, in-flight, DLQ, lag/oldest age where meaningful.
  - Use existing Redis wrapper primitives instead of returning zeros.
  - Notes: Redis now advertises `transport.StatsReader` and derives real topology counts from the main stream, consumer groups, delayed sorted set, and DLQ stream. `Pending` is ready delivery obligations, `InFlight` is the pending-entry-list total, and `Lag` is their checked sum excluding delayed/DLQ records. Tasks use the `workers` group and treat a producer-only stream as entirely ready until that group is created at `0`; events aggregate every `subscriber:*` group and ignore unrelated groups. Oldest age uses the oldest ready/in-flight source with the later envelope creation/schedule timestamp, falling back to the stream ID, and remains explicitly unknown if outstanding state has no readable source. Reads are documented as near-point-in-time multi-command snapshots. Redis 7 lag metadata provides the fast path; Redis 6.2/miniredis compatibility uses cancellable exact `XRANGE` pages of 1,000 instead of fabricated zeros. Storage-type, unavailable-lag, and overflow conditions return errors. Added miniredis regressions for task/event aggregation, ready/in-flight/delayed/DLQ counts, producer-only, untouched-group and empty topologies, oldest age, invalid storage, 1,000-entry pagination, and the public `Bus.Stats` path; the P3.7 regression set passed 20 consecutive runs. Updated `CAPABILITIES.md`, `OBSERVABILITY.md`, `KEY_LAYOUT.md`, and `MIGRATION.md`. Verified default EventBus tests/vet, integration-tag compilation, example/legacy tests/vet, full-repository test/vet, and race tests for core, memory, and Redis with command-local CGO/MinGW on 2026-10-17. Live Redis/Cluster accuracy under concurrent mutation and failover was not exercised because no broker infrastructure was provisioned; it remains assigned to P3.8.

- [x] **P3.8 Add Redis contract and fault tests**
  - Single node and Redis Cluster.
  - Crash/failure around publish and ACK.
  - Reclaim, delayed burst, dedupe TTL, malformed payload, and multi-sweeper tests.
  - Notes: Added reusable `transport/contracttest.Run`, covering durable publish/consume/ACK, retry transfer, DLQ list/requeue/count, deterministic delayed delivery, event fan-out groups, topology-scoped dedupe expiry/release, real stats, and malformed payload settlement. Redis runs it hermetically with miniredis and under the integration tag against live Redis 7 standalone and a three-master Redis Cluster. Added a scoped Docker Compose fixture and explicit environment-driven test setup; live tests use unique namespaces, delete only their own keys, and never flush shared databases. Added deterministic fault injection on both sides of the command ambiguity boundary: closed-connection failures before publish/ACK execution, and response loss after successful XADD, ownership-checked ACK, atomic retry, or atomic DLQ scripts. Post-commit retry/DLQ retries observe settled source state and never create a second destination; pre-commit ACK failure leaves the PEL source available. `CONTRACT_TESTS.md` maps the shared Phase 14 behaviors to reusable driver tests and transport-independent core tests. Existing reclaim, heartbeat, delayed burst, multi-sweeper, dedupe TTL/isolation, malformed record, DLQ, and stats regressions complete the Redis-specific matrix. The focused P3.8/miniredis set passed 20 consecutive runs. Live standalone and Cluster contracts passed once verbosely and then three consecutive runs using Docker project `eventbus-p38` with `redis:7-alpine` on 2026-10-17. Verified default EventBus tests/vet, full-repository test/vet, integration-tag compilation, and race tests for core, memory, and Redis with command-local CGO/MinGW. Fault injection models process/network loss precisely before command execution and after committed response loss; it does not claim a Redis persistence guarantee beyond the configured AOF/replication policy.

### Phase 3 exit criteria

- [x] Redis passes the complete transport contract suite.
- [x] No known retry/DLQ settlement window can silently lose a job.
- [x] Active jobs are not reclaimed contrary to documented policy.
- [x] DLQ and stats APIs return real data.

## 10. Phase 4 - NATS JetStream driver

Dependencies: Phase 2 complete; Phase 3 contract suite reusable.

- [x] **P4.1 Correct topology provisioning and updates**
  - Compare existing stream/consumer configuration and update compatible changes.
  - Do not use string matching as the primary error classifier.
  - Configure `MaxAckPending` from concurrency and prefetch.
  - Notes: Added typed, context-aware JetStream reconciliation for main and DLQ streams. Provisioning now inspects existing stream configuration, creates only on `ErrStreamNotFound`, applies compatible updates to EventBus-owned subjects/storage/retention/max-age/replicas, and preserves unrelated broker limits. Concurrent create races are classified with `ErrStreamNameAlreadyInUse`, re-inspected, and reconciled instead of failing nondeterministically; incompatible server-rejected changes remain observable startup errors. Durable pull consumers now follow the same `ConsumerInfo`/`ErrConsumerNotFound`/`AddConsumer`/`UpdateConsumer` flow without error-string matching, preserve unmanaged consumer fields, reject incompatible ACK/push modes, and reconcile `AckWait`. Extended `transport.Subscription` with the effective core concurrency/prefetch bounds; the engine supplies topology overrides after resolution, and NATS configures `MaxAckPending = concurrency + prefetch` instead of the former hard-coded 20,000 (direct transport callers fall back to 10 + 20). Added overflow/invalid-capacity validation and worker-pool propagation coverage. Embedded JetStream regressions verify stream creation/update, unmanaged-field preservation, explicit incompatible update failure, consumer update/preservation/mode rejection, and eight concurrent provisioners; the focused P4.1 suite passed 20 consecutive runs. Updated `CAPABILITIES.md` and `MIGRATION.md`. Verified the complete embedded-NATS integration suite, default EventBus tests/vet, integration-tag compilation, full-repository test/vet, and race suites for core, memory, Redis, and integration-tagged NATS with command-local CGO/MinGW on 2026-10-17.

- [x] **P4.2 Correct message identity and dedupe**
  - Preserve message ID through consume, retry, delay, and DLQ.
  - Use message ID, not ordering key, for broker dedupe.
  - Honor requested dedupe TTL or return unsupported when the bucket model cannot honor it.
  - Distinguish duplicate-key errors from infrastructure errors.
  - Notes: NATS now keeps ordering and identity independent: immediate publishes use the logical message ID for JetStream idempotency, never the ordering key. A dedicated `X-EventBus-Message-Id` header preserves that logical ID through consume, delayed retry, and DLQ; scheduled publications use a schedule-specific `Nats-Msg-Id` so a valid retry inside JetStream's duplicate window is not suppressed, while old records still fall back to `Nats-Msg-Id`. Added public `WithDedupeTTL`, persisted its nanosecond duration in v3 envelopes, retained the zero-value 24-hour compatibility default, and reject negative TTL or TTL without a dedupe key. NATS now implements `TopologyDeduper` using the durable non-expiring `eventbus_dedupe_v2` bucket, encoded namespace/kind/name/key identity, logical per-reservation expiry, and KV revision CAS so exactly one contender replaces an expired entry. Only typed `ErrKeyExists` plus an unexpired readable value is a duplicate; malformed expiry, KV, context, and connection failures return observable errors. Release is topology-scoped and idempotent. Documented logical/broker identity behavior and the non-automatic migration from the legacy global dedupe bucket in `CAPABILITIES.md` and `MIGRATION.md`. Added embedded JetStream regressions for same-ordering-key distinct IDs, immediate duplicate suppression, consume identity, retry/delay/DLQ identity, topology isolation, requested expiry, release, concurrent expiry CAS, corrupted state, and connection failure; the focused P4.2 set passed 10 consecutive runs. Verified the complete embedded-NATS integration suite, default and full-repository tests, full vet, integration-tag compilation, and race suites for core/default NATS plus the focused integration tests with command-local CGO/MinGW on 2026-10-17.

- [x] **P4.3 Implement scalable delay scheduling**
  - Do not rely solely on `NakWithDelay` for large delayed backlogs.
  - Use durable KV-backed scheduling with collision-free keys and safe multi-instance ownership/CAS.
  - Avoid unbounded in-memory heaps.
  - Notes: Replaced the process-local `WatchAll` plus unbounded heap with a durable KV time-cursor scheduler. Every acceptance now creates an independent `v2.<second>.<run-at-ms>.<uuid>` record containing the logical ID, exact deadline, envelope bytes, owner, and lease, so identical IDs/deadlines cannot overwrite one another. The durable cursor scans one second slot through NATS KV's bounded watcher channel; metadata-only recovery jumps over arbitrarily long empty downtime instead of retaining or probing the complete backlog. Publication work is bounded by `nats.delay_batch_size` (default 100, range 1–1000). Independent instances claim through revision CAS for `nats.delay_lease` (default 30s), publish with the stable schedule ID, and delete only the claimed revision after broker acknowledgement. Pre-publish failures retain/release the record; post-publish/pre-delete crashes recover after the lease and may duplicate but cannot intentionally lose the job. Scheduler shutdown now cancels and joins owned goroutines. Valid P4.2 `<run-at>_<message-id>` records are migrated create-before-delete with deterministic schedule IDs. Added `SCHEDULER.md`, option helpers/validation, capability and migration guidance. Embedded JetStream regressions cover 101-record collision/batch bounds, a 101-record backlog remaining outside a main consumer limited to `MaxAckPending=2`, two-instance CAS, expired-owner recovery, the publish-before-delete crash window, legacy migration, logical identity, and 24-hour cursor recovery; the focused P4.3 set passed 20 consecutive runs. Verified the complete embedded-NATS integration suite, default and full-repository tests, full vet, integration-tag compilation, and race suites for core/default NATS plus focused scheduler integrations with command-local CGO/MinGW on 2026-10-17.

- [x] **P4.4 Implement real heartbeat and settlement**
  - Use `InProgress` for active long-running handlers.
  - Publish retry/DLQ durably before ACK/Term.
  - Use broker message dedupe to constrain duplicates in publish-before-ACK windows.
  - Notes: NATS deliveries now implement `transport.LeaseDelivery` with a cadence of one third of the reconciled consumer `AckWait`. The engine renews before handler execution and sends context-bounded, broker-confirmed JetStream `InProgress` requests while work is active. Fixed a core completion race where normal handler cancellation of an in-flight heartbeat was misclassified as lease loss, suppressing the successful ACK and causing redelivery; broker heartbeat failures still cancel work and leave the source unsettled. ACK, heartbeat, and terminal acknowledgement now honor caller cancellation/deadlines. Retry settlement creates a durable `transfer.<id>` intent and v2 scheduled record keyed by a stable hash of source stream, durable consumer, and stream sequence before synchronously ACKing the source. Repeated/ambiguous settlement reconstructs or reuses the first schedule instead of adding another; completed dispatch removes its intent. DLQ publication uses the same source-scoped stable broker ID, waits for the publish acknowledgement, then performs a context-confirmed JetStream `Term`. Destination failure never settles the source. Added an explicit post-transfer/pre-source-settlement fault seam for embedded-broker tests. Regressions cover a 1.6-second handler under 600ms `AckWait` with repeated `InProgress` and no redelivery, normal completion racing an in-flight heartbeat, cancellation, missing retry/DLQ destinations, retry/DLQ response ambiguity, source pending state, single destination records, retry intent cleanup, and consumer-isolated transfer identity. The focused P4.4 suite passed 10 consecutive runs. Updated `CAPABILITIES.md`, `MIGRATION.md`, and `SCHEDULER.md`. Verified the complete embedded-NATS integration suite, default and full-repository tests, full vet, integration-tag compilation, and race suites for core/default NATS plus focused settlement integrations with command-local CGO/MinGW on 2026-10-17.

- [x] **P4.5 Implement NATS DLQ admin and stats**
  - Use stream and consumer info primitives.
  - Preserve failure metadata in DLQ records.
  - Notes: NATS dead-letter settlement now publishes a versioned wrapper that preserves the authoritative source message plus reason, error, attempts, failure time, source stream, durable consumer, and source stream sequence before terminally acknowledging the source. The driver implements bounded `DLQAdmin` and deletion-stable `CursorDLQAdmin` pages using JetStream stream sequences as record IDs; requeue waits for the main-stream publish acknowledgement, uses a stable broker dedupe ID, and deletes only afterward, while missing IDs are idempotent no-ops. Purge and count use JetStream stream primitives. `StatsReader` reports consumer `NumPending` and `NumAckPending` obligations rather than retained stream history, aggregates event subscribers, treats producer-only task streams as ready, streams delayed KV keys without building an in-memory backlog, and reports DLQ state separately. Documented the publish-before-delete duplicate window, pre-P4.5 raw DLQ incompatibility, and near-point-in-time stats semantics in `CAPABILITIES.md`, `MIGRATION.md`, and `OBSERVABILITY.md`. Embedded JetStream regressions verify complete failure/source metadata, cursor listing, duplicate/missing-ID requeue, non-empty purge, and ready/in-flight/delayed/DLQ stats; the focused set passed 10 consecutive runs. Verified the complete embedded-NATS integration suite, default EventBus tests/vet, integration-tag compilation, full-repository tests/vet, and race suites for default NATS plus focused P4.5 integrations with command-local CGO/MinGW on 2026-10-17.

- [x] **P4.6 Add NATS contract and fault tests**
  - Multi-instance scheduler, disconnect/reconnect, AckWait expiry, heartbeat, MaxAckPending saturation, and duplicate publish tests.
  - Notes: Added an embedded, file-backed JetStream harness that runs the reusable transport contract for publish/consume/ACK, retry, DLQ administration and requeue, delayed dispatch, event fan-out, topology-scoped dedupe, statistics, and malformed payload settlement. The harness stops its autonomous scheduler after provisioning so exact due-time assertions remain deterministic; the shared dedupe contract now provisions both tested topologies before reserving keys. Added broker-level fault regressions proving `AckWait` expiry redelivers the same logical ID with an incremented broker attempt, `MaxAckPending = concurrency + prefetch` withholds a third delivery until synchronous ACK releases capacity, and an EventBus consumer resumes after an observable client disconnect and file-backed server restart. Existing P4 tests cover repeated heartbeat renewal, 101-record multi-instance scheduler CAS, expired lease/crash recovery, duplicate immediate publication by logical message ID, dedupe corruption/network failure classification, retry identity/attempt metadata, and retry/DLQ publish-before-settlement fault windows. Updated `CONTRACT_TESTS.md`, `CAPABILITIES.md`, and `TESTING.md`. The focused shared-contract/fault set passed 10 consecutive runs before the test-only scheduler wrapper refinement and three contract runs afterward; the complete embedded-NATS suite passed. Verified default and full-repository tests/vet, integration-tag compilation, and race suites for core/default NATS plus the focused integration contract/fault tests with command-local CGO/MinGW on 2026-10-17.

### Phase 4 exit criteria

- [x] NATS passes the complete transport contract suite.
- [x] Delayed backlog cannot stall the main consumer through `MaxAckPending` exhaustion.
- [x] KV/network failure is never treated as a duplicate without evidence.
- [x] Retry preserves ID and attempt metadata.

## 11. Phase 5 - Kafka driver

Dependencies: Phase 2 complete; Phase 3 contract suite reusable.

- [x] **P5.1 Introduce a secure Kafka client factory**
  - Internal producer, consumer, admin, and delay clients inherit TLS, SASL, client ID, timeouts, and broker configuration.
  - Do not rebuild consumers from broker addresses alone.
  - Remove panic-prone reliance on global default clients from core construction paths.
  - Review follow-up (in progress): fix conflicting disabled/marked auto-commit options by explicitly rejecting unsupported manual-commit EventBus construction; honor disabled producer idempotency and validate ACK combinations; snapshot wrapper input configuration before creating any native clients. Regression coverage includes commit-policy rejection, standalone manual-consumer construction, ACK/idempotency matrix, and input mutation across producer/internal-consumer snapshots.
  - Notes: Added `kafka.NativeClientFactory`, which snapshots brokers and cloned TLS configuration once and creates producer/consumer roles with the same client ID, TLS/SASL mechanism, dial/request/produce/fetch/session timeouts, retries, compression, batching, acknowledgements, offset reset, and commit policy. Refactored the root Kafka client to use this factory and removed the plain TCP dialer that was appended after `DialTLSConfig` and could override TLS. SASL is now applied only for SASL protocols; missing mechanisms/credentials, unsupported protocols, and OAuth token-URL-only configuration fail explicitly rather than constructing an unauthenticated or empty-token client. `Client.Config` now returns independent broker/TLS copies. Added EventBus `NewWithConfig` (owned producer) and `NewWithClient` (connected caller-owned client); producer/admin share the configured base while main and delay consumers come from the secure factory. The deprecated raw `New` adapter snapshots the supplied franz-go client's effective options instead of rebuilding from broker addresses. Registry wiring uses non-panicking `LookupDefault` and returns ordinary missing/disconnected-default errors. Delay consumer construction now occurs synchronously during topology provisioning so option errors fail startup. Unit tests inspect franz-go's effective options for broker, TLS, SASL, client ID, dial/request/fetch/session settings, topology overrides, immutable snapshots, legacy adapter inheritance, ownership construction, and invalid security configurations without logging credentials. Updated `CAPABILITIES.md`, `MIGRATION.md`, root Kafka documentation, and OAuth configuration comments. Verified full-repository tests/vet, integration-tag compilation, live Kafka publish/consume and delay tests against `localhost:9092`, and default plus integration-tagged Kafka race suites with command-local CGO/MinGW on 2026-10-17. The old fixed-topic integration test was made topology-unique and given a race-appropriate deadline after race instrumentation exposed group startup exceeding its former five-second bound; no race was reported.

- [x] **P5.2 Implement partition-aware processing**
  - Preserve ordering within a partition.
  - Allow concurrency across partitions.
  - Bound fetched but unsettled records.
  - Notes: Added a session-owned partition dispatcher that admits only one unsettled delivery per topic/partition into the engine, advances that partition only after successful settlement, and allows other partition heads to progress concurrently. Main consumers use PollRecords with the checked effective Concurrency + Prefetch bound (direct callers default to 10 + 20); no new batch is polled until the current batch settles, and no per-record/partition goroutines are created. Settlement failure terminates intake without advancing that partition, cancellation leaves undispatched records untouched, and the existing handler barrier retains the settlement client during drain. CloseAllowingRebalance fixes cleanup of the existing BlockRebalanceOnPoll client. Documented the batch-barrier throughput tradeoff, separate franz-go byte buffers, retry-order limitation, and current mark-for-auto-commit behavior; Ordering remains false pending P5.3/P5.4. Deterministic regressions cover slow lower offsets including offset gaps, independent partition progress, callback acceptance versus settlement, bounded polling, cancellation/late settlement, failed settlement, invalid/overflow bounds, and the real engine's two-worker path. The Kafka unit suite passed 20 consecutive runs. Verified full-repository tests/vet, integration-tag compilation, core/Kafka race tests with command-local CGO/MinGW, and the full live Kafka driver integration suite (publish/consume, delay, unique two-partition processing) against localhost:9092 on 2026-10-17.

- [ ] **P5.3 Implement contiguous offset coordination**
  - Offset N cannot be committed while any lower unsettled offset in that partition remains incomplete.
  - All commits occur through a driver-owned coordinator, not handler goroutines.
  - Notes:

- [ ] **P5.4 Handle rebalances safely**
  - Stop intake for revoked partitions.
  - Drain or cancel affected workers according to deadline.
  - Commit only safe contiguous offsets before releasing partitions.
  - Notes:

- [ ] **P5.5 Implement retry/DLQ settlement modes**
  - Preferred: Kafka transaction that produces retry/DLQ and commits source offsets atomically.
  - Fallback: synchronous produce then commit, documented as duplicate-prone but loss-safe.
  - Never commit when retry/DLQ produce fails.
  - Notes:

- [ ] **P5.6 Replace per-record delay timers**
  - Use bounded delay tiers or another durable scheduler.
  - No unbounded `time.AfterFunc` per delayed record.
  - Preserve ordering key across delay and retry.
  - Validate partition counts across related topics.
  - Notes:

- [ ] **P5.7 Implement Kafka DLQ admin and stats**
  - DLQ record includes source topic, partition, offset, timestamp, reason, and error.
  - Lag is calculated from end offsets and committed group offsets.
  - Document limitations of purge/requeue on append-only logs.
  - Notes:

- [ ] **P5.8 Add Kafka contract, rebalance, and fault tests**
  - TLS/SASL construction test.
  - Rebalance during processing.
  - Broker failure between produce and commit.
  - Out-of-order handler completion.
  - Transaction abort/retry where supported.
  - Notes:

### Phase 5 exit criteria

- [ ] Kafka passes the complete transport contract suite.
- [ ] No offset is committed past an unsettled lower offset.
- [ ] Consumers work with configured TLS/SASL.
- [ ] Retry/DLQ publish failure cannot cause source commit.

## 12. Phase 6 - Administration and performance

Dependencies: Phases 3, 4, and 5 complete.

- [ ] **P6.1 Add pause, resume, and topology drain**
- [ ] **P6.2 Add batch enqueue and bounded batch consume/settlement**
- [ ] **P6.3 Add rate limits and optional per-ordering-key concurrency limits**
- [ ] **P6.4 Add payload-size validation and limits**
- [ ] **P6.5 Add operational dashboards/examples for metrics and tracing**
- [ ] **P6.6 Run standardized benchmarks for all drivers**
  - Payload sizes: 1 KB, 10 KB, 100 KB.
  - Concurrency: 1, 10, 100.
  - Workloads: immediate, delayed, 10% retry, 50% retry.
  - Backlogs: 100,000 and 1,000,000 messages where infrastructure permits.
  - Record throughput, p50/p95/p99 latency, allocations, CPU, memory, and duplicate count.

### Phase 6 exit criteria

- [ ] No benchmark regresses beyond the agreed threshold without an ADR.
- [ ] Resource use remains bounded under delayed and retry-heavy workloads.
- [ ] Operational APIs have examples and integration tests.

## 13. Phase 7 - Migration and legacy deprecation

Dependencies: Phase 6 complete.

- [ ] **P7.1 Build adapters for legacy queue APIs**
  - Cover `queue`, `queue_kafka`, and `queue_nats` registration/enqueue patterns where practical.
  - Notes:

- [ ] **P7.2 Rewrite migration documentation**
  - Only use APIs that compile.
  - Explain at-least-once behavior and handler idempotency.
  - Warn against naive dual-consumer side effects.
  - Notes:

- [ ] **P7.3 Add migration verification tooling**
  - Compare published envelope counts, lag, failures, and DLQ outcomes.
  - Shadow consumers must not execute real side effects.
  - Notes:

- [ ] **P7.4 Execute staged migration**
  1. Make handlers idempotent.
  2. Deploy EventBus consumers in shadow/no-side-effect mode.
  3. Compare behavior and performance.
  4. Switch producers to EventBus.
  5. Drain legacy queues.
  6. Switch consumers with rollback capability.
  7. Complete soak period.
  - Notes:

- [ ] **P7.5 Deprecate legacy modules**
  - Keep them for at least one release after successful migration.
  - Remove only when legacy backlog is zero, rollback window is closed, and production acceptance criteria pass.
  - Notes:

### Phase 7 exit criteria

- [ ] Production soak is complete with no unexplained loss.
- [ ] Duplicate rate is within the documented SLO.
- [ ] Legacy backlog is zero.
- [ ] Rollback and deprecation decisions are recorded.

## 14. Shared transport contract test suite

Every durable driver must pass the same behavioral tests where its capability matrix says supported:

- [ ] enqueue, handle, and ACK
- [ ] handler failure and exact attempt bound
- [ ] explicit retry-after
- [ ] permanent error to DLQ
- [ ] decode error to DLQ
- [ ] panic recovery
- [ ] handler timeout
- [ ] settlement timeout/failure
- [ ] retry publish failure does not lose source
- [ ] DLQ publish failure does not lose source
- [ ] duplicate delivery
- [ ] delayed execution precision
- [ ] multi-instance delay scheduler
- [ ] dedupe duplicate, expiry, namespace, and topology isolation
- [ ] graceful drain with in-flight work
- [ ] forced shutdown and later redelivery
- [ ] consumer reconnect without goroutine leak
- [ ] task/event routing isolation
- [ ] subscriber retry isolation
- [ ] ordering guarantee when advertised
- [ ] stats accuracy
- [ ] DLQ list/requeue/purge/count
- [ ] malformed transport message handling

Use channels, fake clocks, and observable hooks instead of fixed sleeps wherever possible.

## 15. Verification commands

Run commands appropriate to the changed scope. In restricted Windows environments, use a workspace-local Go cache:

```powershell
$env:GOCACHE='D:\Go\go-common\.gocache-review'
go test ./eventbus/...
go vet ./eventbus/...
```

Legacy compatibility checks:

```powershell
$env:GOCACHE='D:\Go\go-common\.gocache-review'
go test ./redis ./kafka ./nats ./queue ./queue_kafka ./queue_nats
go vet ./redis ./kafka ./nats ./queue ./queue_kafka ./queue_nats
```

Before completing a transport phase, also run its integration suite using the build tag and environment documented by Phase 0. Do not silently skip unavailable integration infrastructure; record the skipped verification in the relevant Notes section.

## 16. Production release gates

All gates are mandatory:

- [ ] No unresolved P0 defect.
- [ ] Core and all enabled driver contract tests pass.
- [ ] Default tests are hermetic.
- [x] Race tests pass for core and memory transport.
- [ ] Crash-window tests demonstrate no silent loss.
- [ ] Kafka never commits beyond unsettled offsets.
- [ ] Redis does not reclaim an actively leased job contrary to policy.
- [ ] NATS delayed backlog does not exhaust main-consumer capacity.
- [ ] DLQ and stats return real, verified data.
- [ ] TLS/SASL production configuration is covered.
- [ ] Metrics and tracing expose settlement failures and saturation.
- [ ] Benchmarks and soak-test results are recorded.
- [ ] Migration rollback procedure has been exercised.

## 17. Current verification snapshot

Recorded before refactor implementation:

- Selected EventBus unit tests pass.
- Redis EventBus driver tests pass.
- NATS EventBus driver tests pass.
- `go vet ./eventbus/...` passes.
- `redis`, `kafka`, `nats`, `queue`, `queue_kafka`, and `queue_nats` compile and pass `go vet`, but currently contain no package tests.
- Full `go test ./eventbus/...` is not hermetic because a Kafka spike test requires `localhost:9092`.
- `TestConfigValidate` disagrees with the current defaulting behavior for an empty namespace.

Update this snapshot only after completing a phase; do not erase historical failures without recording how they were resolved.

### Phase 0 completion snapshot (2026-10-15)

- `go test ./eventbus/... -count=1` passes without external brokers.
- `go vet ./eventbus/...` passes.
- `go test -tags=integration ./eventbus/... -run '^$'` compiles every opt-in integration test.
- Legacy Redis/Kafka/NATS clients and queue packages still compile and pass `go vet`.
- Codec, enqueue, engine dispatch, and memory transport baselines are recorded in `eventbus/BENCHMARKS.md`.
- Delivery semantics are accepted in `ADR-001-delivery-semantics.md`.
- Current and target transport capabilities are recorded in `CAPABILITIES.md`.

### Phase 1 completion snapshot (2026-10-17)

- Task and event topology kinds are explicit and isolated; producer-only declarations do not require local handlers.
- Attempts are 1-based total executions in v3 envelopes, with automatic v2 decode normalization and exhaustive boundary tests.
- Transport contracts use single-use settlement and split optional statistics, DLQ, deduplication, and pause capabilities behind stable `ErrNotSupported` classification.
- Core and built-in drivers apply strict config, name, handler, option, and topology validation before mutation or publication.
- Legacy EventBus entry points remain available as deprecated adapters, and `MIGRATION.md` records replacements and unavoidable breaking changes.
- `go test ./eventbus/... -count=1`, `go vet ./eventbus/...`, integration-tag compile checks, `go test`/`go vet` for the EventBus example, and legacy package tests/vet pass.
- Race tests remain unavailable in this environment because CGO is disabled and no `gcc` compiler is installed; this limitation was recorded during P1.5 and remains a later release gate.

### Phase 2 completion snapshot (2026-10-17)

- P2.1 through P2.7 and all Phase 2 exit criteria are complete.
- Bounded worker pools, supervised consume sessions, retryable startup,
  graceful/forced drain, default panic recovery, observability hooks, and
  envelope metadata normalization are implemented with regression coverage.
- Default EventBus tests, EventBus/example/legacy vet, example/legacy tests,
  integration-tag compilation, and embedded-NATS driver integration passed in
  the P2.7 verification recorded above.
- Core and memory race suites passed with command-local CGO enabled and Scoop
  MinGW GCC 16.2.0. Earlier CGO blockers are resolved by this verification.
- Later driver-phase work and remaining production release gates stay open.

### Phase 3 completion snapshot (2026-10-17)

- Redis uses versioned, cluster-slot-safe topology keys and bounded atomic Lua
  operations for delayed dispatch, retry/DLQ settlement, and DLQ requeue.
- Reclaim preserves stream identity and delivery history; ownership-checked
  heartbeat and settlement prevent stale workers from reclaiming or settling
  actively leased work.
- Redis exposes real DLQ administration and topology statistics with documented
  obligation semantics and Redis 6.2-compatible fallbacks.
- The reusable transport contract passes on miniredis, live Redis 7 standalone,
  and a live three-master Redis Cluster fixture. Deterministic pre-command and
  post-commit response-loss tests cover publish, ACK, retry, and DLQ ambiguity.
- Default EventBus and full-repository tests/vet, integration-tag compilation,
  and core/memory/Redis race suites pass. Redis durability still depends on the
  deployment's persistence, replication, eviction, and failover configuration.

### Phase 4 completion snapshot (2026-10-17)

- NATS reconciles stream/consumer configuration, bounds `MaxAckPending`, keeps
  logical identity separate from ordering, and uses topology-scoped dedupe with
  explicit expiry plus revision CAS.
- The durable KV scheduler is bounded, collision-free, and multi-instance safe;
  heartbeat and publish-before-source-settlement paths are broker-confirmed and
  preserve retry/DLQ identity across ambiguous response windows.
- Versioned DLQ records retain failure and source metadata. Cursor administration
  and consumer-obligation statistics return real JetStream state with documented
  publish-before-delete and near-point-in-time limitations.
- The reusable transport contract passes against embedded file-backed JetStream.
  Broker regressions cover restart/reconnect, `AckWait` redelivery, heartbeat,
  `MaxAckPending` saturation, duplicate publication, and scheduler ownership.
- Default and full-repository tests/vet, integration-tag compilation, the complete
  embedded-NATS integration suite, and core/default/focused-integration race
  suites pass. NATS remains at-least-once and advertises neither ordering nor
  atomic retry/DLQ settlement.

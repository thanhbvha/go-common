# EventBus Refactor Execution Plan

> Status: Phase 6 verified complete; Phase 6.7 hardening planned; Phase 7 not started
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

- [x] **P5.3 Implement contiguous offset coordination**
  - Offset N cannot be committed while any lower unsettled offset in that partition remains incomplete.
  - All commits occur through a driver-owned coordinator, not handler goroutines.
  - Notes: Main Kafka consumers now disable auto-commit through a dedicated secure factory role and send every handler settlement to a session-owned coordinator. The coordinator registers each bounded fetch in partition order, permits compacted offset gaps, waits behind lower unsettled records, synchronously commits only the highest consecutively settled record, and reports success only after franz-go receives broker confirmation. Independent partitions progress separately; commit failure leaves the offset registered and retryable, and the handler/drain barrier retains the coordinator until accepted work finishes. Added deterministic regressions for out-of-order completion, cross-partition progress, broker-confirmation blocking, commit failure/retry, late settlement after intake cancellation, main-consumer manual-commit options, and TLS/SASL inheritance through both configured and legacy factories. Updated `CAPABILITIES.md`, `MIGRATION.md`, and the Kafka factory documentation; ordering remains unadvertised until P5.4 makes rebalance ownership transitions safe. Verified the Kafka/wrapper suites 20 consecutive times, default EventBus tests/vet, integration-tag compilation, full-repository tests/vet, and core/Kafka race tests with command-local CGO/MinGW on 2026-10-18. After Kafka became available at `localhost:9092`, added a broker-backed regression that observes the real consumer-group offset remain `-1` after only the higher record completes, then advance exactly to the highest contiguous offset plus one after the lower record settles. This regression passed 10 consecutive live runs and a live race-enabled run; the complete tagged Kafka suite also passed on 2026-10-18.

- [x] **P5.4 Handle rebalances safely**
  - Stop intake for revoked partitions.
  - Drain or cancel affected workers according to deadline.
  - Commit only safe contiguous offsets before releasing partitions.
  - Notes: Added a session rebalance controller around franz-go's `BlockRebalanceOnPoll` lifecycle. `OnPartitionsCallbackBlocked` immediately stops dispatching new records while already accepted partition heads drain under the engine's existing handler and settlement deadlines; only after those settlements receive synchronous broker confirmation does the consumer call `AllowRebalance`. Because `PollRecords` advances local fetch positions for the whole bounded batch, the driver now rewinds every retained partition to its first polled-but-undispatched record before releasing the callback, preventing silent skips when a cooperative rebalance retains ownership. Assignment callbacks enable coordinator ownership tracking, while revoke/lost callbacks fence and remove affected offsets so stale deliveries cannot commit after ownership changes; shutdown keeps the coordinator alive through the final revoke callback. Kafka now advertises source-partition ordering, with the existing DS-52 retry-order limitation documented. Deterministic tests cover stop-intake, in-flight drain, rewind positions, allow-after-settlement, revoke/lost fencing, and ordering capability; the Kafka unit suite passed 20 consecutive runs. A live two-member/two-partition Range-balancer regression forces rebalance during in-flight work and verifies drain-before-transfer, no duplicate/lost successor, and delivery by the new owner; it passed 10 consecutive runs plus a race-enabled run against `localhost:9092`. Verified the complete tagged Kafka suite, default EventBus tests/vet, integration-tag compilation, core/Kafka race tests, and full-repository tests/vet on 2026-10-18. One parallel full-repository run exposed the unrelated timing-sensitive cron test executing once instead of its expected two or three times; the focused cron test then passed three consecutive runs and the full suite passed when rerun sequentially.

- [x] **P5.5 Implement retry/DLQ settlement modes**
  - Preferred: Kafka transaction that produces retry/DLQ and commits source offsets atomically.
  - Fallback: synchronous produce then commit, documented as duplicate-prone but loss-safe.
  - Never commit when retry/DLQ produce fails.
  - Notes: Explicitly selected the permitted synchronous publish-before-commit fallback. A shared confirmed-production path preserves payload, headers, and ordering key for immediate/delayed retries and normalized/raw DLQ records, then requests source settlement exclusively through the existing contiguous coordinator. Destination failure or ambiguous publication never requests source commit; commit failure remains observable and retryable. EventBus rejects unacknowledged (`acks=0`) and externally transactional producers with `ErrNotSupported`, including deferred validation/publish/consume errors for the deprecated raw constructor. Atomic transaction mode is not implemented or advertised: independent consumer groups need session-owned transactional producers, fencing/abort recovery, and read-committed main/delay consumers before it can be offered safely. `driver/kafka/SETTLEMENT.md`, `CAPABILITIES.md`, `MIGRATION.md`, and `TESTING.md` document that choice, broker durability requirements, duplicate windows, and the unchanged P5.6 scheduler boundary. Deterministic tests cover publish/commit failures, ambiguous publish responses, confirmation barriers, cancellation after publication, higher-offset blocking after a failed lower transfer, metadata/routing, and single-use settlement. The Kafka unit suite passed 20 consecutive runs. Eight live Kafka fault cases inject failures before/after destination production and source commit, checking real group offsets and destination log counts for retry/DLQ; the full tagged Kafka suite and race-enabled fault suite passed against localhost:9092. Verified integration-tag compilation, core/Kafka race tests with command-local CGO/MinGW, and full-repository vet on 2026-10-18. The first parallel full-repository test run hit an unrelated Redis `TestDelayedDispatchMultipleSweepers` I/O timeout; that focused test then passed three consecutive runs and the full suite passed with `go test -p 1 ./...`.

- [x] **P5.6 Replace per-record delay timers**
  - Use bounded delay tiers or another durable scheduler.
  - No unbounded `time.AfterFunc` per delayed record.
  - Preserve ordering key across delay and retry.
  - Validate partition counts across related topics.
  - Notes: Replaced the per-record `time.AfterFunc` path with three durable Kafka delay tiers (`5s`, `1m`, and `5m`) and one periodic manual-commit worker per tier/topology. Each sweep polls at most `kafka.delay_batch_size` records (default 100, validated range 1-1000); records that still belong in the current tier are rewound and retained in Kafka rather than held by process-local timers. Partition queues are processed in offset order, and malformed/current-tier/publish-failed/commit-failed records stop only their source partition for that sweep. Transfers preserve the exact envelope bytes, broker headers, and ordering key, synchronously confirm the lower-tier/main publish, and only then synchronously commit the source; ambiguous commit windows remain duplicate-prone but loss-safe. `EnsureTopology` waits for metadata visibility and rejects unequal partition counts across main, DLQ, and every delay tier before starting workers. Shutdown cancels and joins all tier workers within the caller's context. Added `driver/kafka/SCHEDULER.md` and updated capability, migration, testing, and wrapper documentation. Deterministic tests cover tier boundaries, bounded polling, shutdown joining, rewinds, publish/commit failures, malformed records, metadata preservation, and option validation. Live Kafka tests verify partition-mismatch rejection and a bounded five-record burst with stable ordering-key partitioning. The Kafka unit suite passed 20 consecutive runs; the full tagged Kafka suite, integration-tag compilation, EventBus/legacy tests and vet, core/Kafka race tests with command-local CGO/MinGW, and sequential full-repository tests/vet passed on 2026-10-18. One initial full EventBus run missed the unrelated C8 shutdown timing threshold by less than one millisecond; its focused test passed three consecutive runs and the complete suite then passed on rerun.

- [x] **P5.7 Implement Kafka DLQ admin and stats**
  - DLQ record includes source topic, partition, offset, timestamp, reason, and error.
  - Lag is calculated from end offsets and committed group offsets.
  - Document limitations of purge/requeue on append-only logs.
  - Notes: Kafka DLQ settlement now writes a versioned wrapper containing the normalized/original message, reason, error, attempts, failure time, and source topic/partition/offset/record timestamp. Kafka advertises real `DLQAdmin` and `StatsReader` implementations. DLQ administration uses stable `<partition>:<offset>` IDs and a dedicated consumer-group offset as an append-only logical cursor: list/count exclude already administered prefixes, requeue requires complete per-partition prefixes and confirms every main-topic publish before advancing the cursor, and purge advances a snapshot cursor without claiming physical log deletion. Publish or cursor-commit ambiguity remains duplicate-prone but loss-safe; cross-process admin mutations must be serialized. Stats calculate task/event and durable-delay lag from end offsets minus committed group offsets, clamp absent/expired commits to log starts, aggregate event subscriber groups, and keep delayed/DLQ separate. Kafka cannot distinguish fetched-uncommitted from never-fetched records through group offsets, so main lag is reported as `Pending`, `InFlight` is zero, and nonempty oldest age is unknown. Added `driver/kafka/ADMIN.md` plus deterministic coverage for wrapper/source metadata, bounded positional scans, prefix/idempotency rules, publish/commit failures, logical purge, task/event/delay lag, and group filtering. The Kafka unit suite passed 20 consecutive runs; default EventBus and legacy tests/vet, integration-tag compilation, the Kafka race suite with command-local CGO/MinGW, and sequential full-repository tests/vet passed on 2026-10-18. The focused live P5.7 admin/stats test subsequently passed against `localhost:9092` during P5.8 verification.

- [x] **P5.8 Add Kafka contract, rebalance, and fault tests**
  - TLS/SASL construction test.
  - Rebalance during processing.
  - Broker failure between produce and commit.
  - Out-of-order handler completion.
  - Transaction abort/retry where supported.
  - Notes: Added a live Kafka harness for the complete shared transport contract, with autonomous tier workers stopped for deterministic due-time dispatch and exact test-topic cleanup after transport shutdown. The capability-gated dedupe scenario is skipped because Kafka does not advertise `TopologyDeduper`; every applicable publish/consume/ACK, retry, DLQ admin/requeue, delayed delivery, event fan-out, stats, and opaque malformed-payload scenario passes. The shared harness now keeps session-owned consumers alive through settlement and evaluates due deadlines at the transport's advertised precision, preserving NATS coverage. Contract testing exposed and fixed two Kafka boundary defects: direct DLQ readers now use a group-free factory role while retaining TLS/SASL and timeout options, and delayed scheduling stores its deadline in a private transport header so arbitrary payload bytes remain opaque (with legacy envelope fallback). TLS/SASL construction is covered for producer, grouped/manual/delay consumers, and direct readers through supported and deprecated constructors. Existing live tests cover out-of-order contiguous commits, in-flight rebalance/drain/partition transfer, and eight retry/DLQ broker fault windows before/after produce and commit. Transactions remain explicitly unsupported and unsafe transactional producers are rejected, so transaction abort/retry is not applicable. Twenty repeated Kafka/Redis/factory unit runs, the NATS and Kafka shared contracts, the complete tagged Kafka suite, integration-tag compilation, default EventBus tests/vet, focused integration race tests, and sequential full-repository tests/vet passed on 2026-10-18 against `localhost:9092`.

### Phase 5 exit criteria

- [x] Kafka passes the complete transport contract suite.
- [x] No offset is committed past an unsettled lower offset.
- [x] Consumers work with configured TLS/SASL.
- [x] Retry/DLQ publish failure cannot cause source commit.

## 12. Phase 6 - Administration and performance

Dependencies: Phases 3, 4, and 5 complete.

- [x] **P6.1 Add pause, resume, and topology drain**
  - Notes: Added portable process-local `Bus.Pause`, `Bus.Resume`, and `Bus.DrainTopology` controls backed by one concurrency-safe session controller per task or event subscriber. Pause cancels broker intake without canceling accepted handler/settlement contexts; topology drain waits until each affected session has joined its bounded worker pool, while a caller deadline returns without falsely marking work complete. Resume wakes paused supervisors and creates fresh consume sessions without restarting the bus. Producer-only topologies return `ErrNotSupported`, pre-start calls return `ErrBusNotRunning`, and global drain still wakes paused supervisors. Deterministic tests cover pre-start rejection, no reconnect while paused, resumed session creation, accepted-handler drain/settlement, and continued bus operation; 20 repeated runs plus focused race, default EventBus/example tests, and vet passed on 2026-10-18.
- [x] **P6.2 Add batch enqueue and bounded batch consume/settlement**
  - Notes: Added ordered `Bus.EnqueueBatch` with a hard 1,000-item bound and explicit non-atomic `BatchError.Index` semantics: each item uses the normal validation, tracing, dedupe, delay, and broker-confirmed publish path; processing stops at the first failure so the durable prefix and unattempted suffix are unambiguous. Consumer-side batches cannot exceed the core `Concurrency + Prefetch` bound: Redis caps fresh `XREADGROUP` and `XAUTOCLAIM` pages, NATS caps pull fetches, Kafka retains its existing `PollRecords` capacity limit, and every record remains settled independently. Deterministic tests cover empty/oversized batches, ordered success, partial failure, and Redis/NATS capacity clamping; focused tests passed 20 consecutive runs and default EventBus/example tests and vet passed on 2026-10-18.
- [x] **P6.3 Add rate limits and optional per-ordering-key concurrency limits**
  - Notes: Added strictly validated `WithRateLimit` token-bucket admission and `WithOrderingKeyConcurrency` per-key semaphores for each local task/subscriber. Admission waits keep broker heartbeats active but begin the handler processing timeout only after admission; cancellation leaves the source unsettled for durable redelivery. Empty ordering keys bypass keyed limits, different keys remain independent, and reference-counted entries are removed after completion/cancellation so attacker-controlled key cardinality does not create retained state. The controls are explicitly process-local and do not advertise cross-instance ordering. Deterministic validation, burst/cancellation, key-isolation, waiter-cancellation, and state-reclamation tests passed 20 consecutive runs plus focused race verification on 2026-10-18.
- [x] **P6.4 Add payload-size validation and limits**
  - Notes: Added a strictly validated 1 MiB `Config.MaxPayloadBytes` default plus positive `WithMaxPayloadBytes` topology overrides. Producers measure the final serialized v3 envelope after trace injection and reject it with `ErrPayloadTooLarge` before ordering checks, dedupe reservation, or broker I/O. Consumers enforce the effective topology limit against raw bytes before heartbeat/network renewal and JSON decoding, then settle oversized sources to DLQ with stable reason `payload_too_large`. This applies uniformly to every driver while remaining independent of smaller broker-side limits. Deterministic tests cover exact-boundary acceptance, one-byte rejection without publish, topology override, pre-decode DLQ routing, invalid configuration/options, 20 repeated runs, and focused race verification on 2026-10-18.
- [x] **P6.5 Add operational dashboards/examples for metrics and tracing**
  - Notes: Added a runnable OTLP example with complete `Metrics`/`RuntimeMetrics` and `Tracer`/`TraceInjector`/`TraceRecorder` adapters, bounded low-cardinality OTel instruments, W3C propagation, producer/consumer span kinds, batch publication, local pause/drain/resume, rate/key admission, payload limits, stats sampling, and graceful telemetry shutdown. Added an importable Grafana dashboard for publish/completion rates, p95 handler/saturation/queue age, topology state, failures, reconnects, settlement errors, and oldest age. `OPERATIONS.md` defines the metric mapping, backend name-translation caveat, initial alerts, stats semantics, trace triage, and local-control scope. Adapter interface assertions, construction/name validation, default example tests, and vet passed on 2026-10-18.
- [x] **P6.6 Run standardized benchmarks for all drivers**
  - Payload sizes: 1 KB, 10 KB, 100 KB.
  - Concurrency: 1, 10, 100.
  - Workloads: immediate, delayed, 10% retry, 50% retry.
  - Backlogs: 100,000 and 1,000,000 messages where infrastructure permits.
  - Record throughput, p50/p95/p99 latency, allocations, CPU, memory, and duplicate count.
  - Notes: Added a common JSON benchmark runner and 36-case PowerShell matrix for Memory, Redis, NATS, and Kafka, with exact payload/concurrency/workload validation, broker-aware delay, deterministic retry mixes, bounded normal-run outstanding work, optional paused backlog preload, publish-error retry, latency percentiles, throughput, allocations, sampled peak heap, Go CPU, GC, and duplicate accounting. All 144 live local smoke cases completed with zero duplicates and bounded sampled heap (maximum 15.2 MB at 20 messages/case); representative 100-message results and complete ranges are recorded in `BENCHMARKS.md`. Re-ran the five core micro-benchmarks three times. Codec/raw-transport latency remained within 20%, while cumulative correctness/observability costs made Enqueue and EngineDispatch exceed the threshold; accepted rationale and the new baseline are in `PERFORMANCE_BASELINE_ADR.md`. Persistent 100k/1m preload was not safe on shared local brokers because the benchmark has no destructive topology cleanup, and Memory is intentionally capped at 1,000 buffered records; the runner supports these sizes for disposable infrastructure without claiming they ran here.

### Phase 6 exit criteria

- [x] No benchmark regresses beyond the agreed threshold without an ADR.
- [x] Resource use remains bounded under delayed and retry-heavy workloads.
- [x] Operational APIs have examples and integration tests.

## 13. Phase 6.7 - Pre-migration safety and production hardening

Dependencies: Phase 6 complete.

This phase resolves the findings in
[`PRE_PHASE7_REVIEW.md`](PRE_PHASE7_REVIEW.md) before compatibility adapters or
shadow production traffic make the remaining behavior harder to change. Finding
IDs below refer to that review. Complete the tasks in order unless a task notes
otherwise; Phase 7 depends on the complete Phase 6.7 exit criteria.

- [x] **P6.7.1 Close no-loss and routing invariant gaps**
  - Fix the dedupe reserve/publish/release ambiguity (EB-01). A failed publish
    plus failed reservation cleanup must never turn a later retry into a
    successful duplicate response when no durable message exists. Cleanup must
    be time-bounded and its ambiguous outcome observable.
  - Remove unsafe ready-stream trimming from Redis immediate publication or
    replace it with an observable retention process proven safe against every
    relevant task/event consumer group (EB-02). Lossy retention must be an
    explicit, separately named mode and cannot advertise the durable contract.
  - Replace separator-joined broker resource names with a versioned, injective
    component encoding and document migration/alias handling for existing
    Memory, NATS, and Kafka resources (EB-03).
  - Add deterministic fault tests for dedupe cleanup, Redis backlog retention,
    and hostile topology names.
  - Acceptance: every successful publication is observable by every intended
    group, and distinct `(namespace, kind, name, subscriber)` tuples cannot map
    to the same physical resource.
  - Notes: Added `transport.PublicationDeduper` with owned reserved/published
    states and stable `ErrDedupeAmbiguous`. Core cleanup/confirmation is bounded
    to five seconds outside caller cancellation; only broker-confirmed
    publication is a successful duplicate, while cleanup/confirmation faults
    remain visible. Memory, Redis Lua, and NATS KV/CAS implementations preserve
    ownership across expiry races; combined publish/cleanup fault regressions
    prove a retry cannot become false success. Redis immediate publication no
    longer uses `MAXLEN`; the legacy `redis.maxlen`/`WithMaxLen` request is
    explicitly rejected, and a hermetic 100,001-record regression proves both
    fast and offline event groups can observe the complete accepted backlog.
    Memory, NATS, and Kafka now use a versioned length-framed Base32 `eb2`
    encoding for topology tuples and subscriber groups. Explicit
    `WithLegacyResourceNames` preserves old resource/group names for supervised
    migration only; no automatic alias is attempted. Separator, task/event,
    namespace/name, Unicode, and subscriber collision cases have deterministic
    coverage; embedded JetStream cross-delivery isolation passed. Updated
    migration, capability, Redis layout, and benchmark guidance. Verified
    `go test ./... -count=1`, `go vet ./...`, default EventBus tests/vet,
    integration-tag compilation and full tagged EventBus execution, the full
    embedded-NATS integration suite, and race tests for core/Memory/Redis on
    2026-10-19. The new live Kafka isolation test compiled but skipped because
    `localhost:9092` refused the connection; live Redis standalone/Cluster
    suites remained environment-gated because their address variables were not
    set. These unavailable broker runs are recorded rather than claimed.

- [x] **P6.7.1R Implement bounded, group-safe Redis retention**
  - Treat the removal of publish-time `MAXLEN` as the completed no-loss fix,
    not as a complete storage lifecycle. Implement the design in
    [`driver/redis/RETENTION.md`](driver/redis/RETENTION.md) before claiming
    bounded Redis resource use or production readiness.
  - Add durable subscription provisioning before consumer startup. Persist the
    intended task/event groups and a manifest epoch in the topology hash slot;
    missing, unmanaged, or concurrently changing relevant groups must block
    trimming rather than weaken delivery guarantees.
  - Compute a conservative global acknowledged watermark from every active
    group's last-delivered position and oldest pending entry. Reclaim only IDs
    strictly below that watermark using bounded `XTRIM MINID` passes; never
    reintroduce count- or age-only lossy trimming.
  - Use one joined transport-owned coordinator, fair bounded batches, and a
    distributed lease only as a duplicate-work optimization. Manifest epoch
    fencing, not lease ownership, protects correctness across instances.
  - Add atomic ready/delayed/DLQ admission limits and a stable typed capacity
    error. When a slow/offline group blocks retention, reject new acceptance
    visibly before the configured bound instead of deleting owed work or
    growing without limit. Keep Redis `noeviction` deployment guidance.
  - Add explicit safe and forced subscriber-retirement administration. Normal
    process shutdown never retires a durable group; forced retirement reports
    the pending/unread work it abandons.
  - Expose stream/capacity usage, watermark, trim progress/errors, blocking
    group/reason, manifest conflicts, and capacity rejection metrics plus
    readiness degradation. Do not log payloads or credentials.
  - Acceptance: live standalone and Cluster tests prove no unread/pending entry
    is trimmed, acknowledged entries are eventually reclaimed, group-addition
    races fail closed, capacity is enforced under an offline subscriber, and
    restart/failover/multi-instance compaction remains loss-free. The 100k/1m
    P6.7.6 runs must show a bounded memory plateau after acknowledgement.
  - Notes: Added engine-level durable subscription provisioning before consumer
    startup and Redis same-slot group manifests with monotonic epochs, active /
    retiring state, missing/unmanaged-group fail-closed checks, and explicit
    safe or forced retirement that reports pending and unread work. Added one
    joined transport-owned retention coordinator, stable fair topology order,
    an optimization lease, conservative last-delivered/oldest-pending
    watermarks, and an atomic epoch plus current-group reconciliation fence.
    Exact bounded `XTRIM MINID` passes scan at most batch+1 eligible IDs and
    remove at most the configured batch, avoiding approximate trimming that can
    retain small acknowledged backlogs forever. Immediate publish, delayed
    admission/dispatch, retry, DLQ settlement, and DLQ requeue now enforce
    same-slot atomic count limits and return typed `ErrCapacityExceeded` without
    removing a durable source. Capacity rejection wakes retention; payload-free
    `RetentionReport` exposes usage/limits, watermark, progress, errors,
    blocking reason, group drift, epoch/lease counters, rejection counts, and
    degraded readiness. Updated `RETENTION.md`, `KEY_LAYOUT.md`, migration and
    capability guidance, including Redis `noeviction`. Deterministic tests cover
    provisioning order, unread/pending protection, exact batch bounds, eventual
    small-backlog reclamation, provisioned and out-of-band group-addition races,
    concurrent capacity, delayed/retry/DLQ source preservation, retirement,
    status continuity, and coordinator shutdown. Live Redis 7 standalone and
    three-master Cluster retention suites passed against the Docker fixture,
    proving offline-group backpressure, multi-instance compaction, manifest
    survival across transport restart, and cross-slot-safe epoch fencing.
    Verified `go test ./... -count=1`, `go vet ./...`, integration-tag
    compilation, and race tests for core/Redis on 2026-10-19. Broker-process
    crash/failover and the required 100k/1m memory-plateau evidence remain
    explicitly assigned to P6.7.6 production-scale/fault execution; they are not
    claimed by this implementation completion.

- [x] **P6.7.2 Harden Memory and define its parity boundary**
  - Treat Memory as a bounded, process-local reference transport. It must match
    all transport-independent EventBus semantics that it advertises, but must
    not claim broker durability, restart recovery, cross-process coordination,
    or distributed ordering (EB-04).
  - Replace the fixed, silently dropping channel behavior with configurable
    per-topology/group capacity and one documented policy: a stable typed
    capacity error or context-bounded backpressure. A `nil` publish result must
    mean the message is observable by every intended group; partial fan-out and
    ambiguous acceptance must be returned explicitly and covered by retry-safe
    tests.
  - Provision declared event subscriber groups before publication rather than
    when `Consume` first starts. Define declaration/redeclaration behavior so an
    event published after successful setup cannot disappear merely because its
    local consume session has not started yet.
  - Replace one goroutine/timer per delayed message with a single owned,
    capacity-bounded scheduler (for example, a priority queue plus timer). Make
    scheduler admission errors synchronous, surface dispatch failures, and join
    the scheduler on `Drain`/`Close`; document that accepted delayed entries are
    still process-local and do not survive process termination.
  - Scope dedupe reservations by physical topology and expiry, and apply the
    same reserve/publish/release ambiguity rules as other dedupe drivers. Do not
    let two tasks/events sharing a caller key suppress each other.
  - Verify real in-process retry, delayed delivery, DLQ list/count/requeue/purge,
    stats, settlement/redelivery, fan-out isolation, and lifecycle behavior.
    Return `ErrNotSupported` for any broker/distributed operation Memory cannot
    implement instead of returning a successful zero value.
  - Add deterministic tests for capacity saturation and cancellation, atomic or
    explicitly ambiguous fan-out, publish-before-consume, concurrent publish,
    topology-scoped dedupe, delayed capacity/order/close, retry and DLQ identity,
    stats, drain, and post-close behavior. Include Memory in every applicable
    shared contract test, while capability-gating durable-only scenarios.
  - Update `CAPABILITIES.md`, `TESTING.md`, examples, and benchmarks to separate
    API/semantic parity from durability. State that Memory is intended for
    tests, local development, and explicitly accepted ephemeral workloads—not
    as a durable substitute for Redis, NATS, or Kafka.
  - Acceptance: Memory has no silent-success loss path, resource use and
    goroutine count remain bounded, every advertised in-process capability has
    contract evidence, unsupported distributed guarantees are machine-readable,
    and restart loss is explicit in documentation and examples.
  - Notes: Replaced Memory's fixed silently-dropping channels with bounded
    per-topology/subscriber state queues. Ready capacity includes queued and
    in-flight obligations (default 1,000 per group); delayed and DLQ stores have
    independent 1,000-entry defaults. Positive `memory.max_ready_entries`,
    `memory.max_delayed_entries`, and `memory.max_dlq_entries` options override
    them. Publication validates every target first and atomically fans out or
    returns typed `ErrCapacityExceeded`, so `nil` means every intended group has
    an observable copy. Memory now implements `SubscriptionProvisioner`; engine
    startup provisions event groups before consumption/publication, and direct
    consumers use the same idempotent path. Replaced one goroutine/timer per
    delayed item with one transport-owned bounded heap/timer scheduler, with
    synchronous admission errors, source-preserving dispatch backpressure, and
    joined close. Payload-free scheduler status exposes pending work, last
    run/success/error, and retry count. Consumption tracks queued/in-flight state and redelivers
    unsettled work after session cancellation; graceful drain uses the existing
    handler barrier before deciding redelivery. Retry and DLQ requeue preserve
    the source event subscriber rather than fanning out again. DLQ transfer and
    requeue are capacity-checked before source removal, and stats now report
    ready, in-flight, delayed, DLQ, lag, and oldest age. Memory implements
    topology-scoped legacy dedupe plus confirmed publication state; all state
    remains process-local and expires or disappears with the process. Read-only
    stats/DLQ list/count remain available after close for final inspection while
    publication, consumption, provisioning, mutation, and dedupe reject the
    closed transport. Event publication without any process-local provisioned
    subscriber and delivery heartbeat return `ErrNotSupported` rather than a
    successful zero value. Added Memory to the reusable transport contract and
    deterministic tests for atomic/ concurrent capacity, publish-before-consume,
    fan-out, scheduler order/capacity/close, redelivery, retry/DLQ subscriber
    isolation, DLQ failure/requeue, stats, dedupe scope, lifecycle, and goroutine
    bounds. Focused Memory tests passed 20 consecutive runs and core/Memory race
    suites passed on 2026-10-19. Benchmark smoke measured EngineDispatch at
    14,859 ns/op / 6,390 B/op / 69 allocs and MemoryTransportRoundTrip at 3,042
    ns/op / 1,333 B/op / 20 allocs on Linux/amd64 i7-12700F; the intentionally
    higher correctness cost is accepted in the performance ADR addendum rather
    than compared directly with the Windows baseline. Updated capabilities, migration, testing,
    contract mapping, examples, and benchmarks to state that Memory is for tests,
    local development, and explicitly accepted ephemeral workloads only; it
    provides no process-restart durability, cross-process coordination,
    distributed ordering, or durable broker lease.

- [x] **P6.7.3 Correct and observe durable schedulers**
  - Redesign Kafka delayed scheduling so a far-future partition head cannot
    indefinitely block a later near-future deadline, or explicitly reject the
    unsupported deadline pattern and narrow the advertised guarantee (EB-05).
  - Add tests with far-future then near-future records in the same Kafka
    partition and verify the documented delivery bound under bounded backlog.
  - Expose Redis, NATS, and Kafka scheduler poll/dispatch/transfer/commit errors,
    retry counts, last-success time, and degraded health through bounded
    observability hooks (EB-08). Scheduler loops must not silently discard
    non-cancellation errors.
  - Make Redis cancellation join every topology sweeper within the caller's
    shutdown deadline, matching the NATS/Kafka lifecycle contract (EB-09).
  - Acceptance: advertised precision states all preconditions, scheduler faults
    are alertable, and `Close` cannot return while an owned scheduler is still
    using broker resources.
  - Notes: Kafka delay workers now poll every five seconds and remove
    partition head-of-line blocking by broker-confirmed rotation of a
    not-yet-due head to the tail of the same durable tier before committing its
    source. Later near-future deadlines in the same partition can therefore
    move to a lower tier in the same bounded batch; publish/commit ambiguity
    remains duplicate-prone but loss-safe. The documented five-second precision
    now requires a healthy scheduler and at most one bounded poll ahead of the
    record; larger backlogs add bounded sweep intervals and synchronous transfer
    time. Added optional `transport.SchedulerHealthReader` with payload-free
    per-worker snapshots for Redis, NATS, and Kafka, including poll/dispatch/
    transfer/commit/retry counters, last poll/success/error times, sanitized and
    bounded error type, consecutive failures, and degraded state. Autonomous
    loops now retain non-cancellation failures in alertable health instead of
    silently discarding them, and successful sweeps clear degradation. Redis
    now tracks every topology delay sweeper in a WaitGroup; `Close` cancels and
    joins all sweepers plus the retention worker within the caller deadline and
    cannot report success while one still uses Redis. Deterministic regressions
    cover far-future then near-future records in one Kafka partition, health
    failure/recovery for all three durable schedulers, bounded error exposure,
    and Redis close deadline/join behavior. Updated `CAPABILITIES.md`,
    `OBSERVABILITY.md`, `TESTING.md`, and Kafka `SCHEDULER.md`. Focused Kafka/
    Redis regressions passed 20 consecutive runs; the complete embedded-NATS
    integration suite passed; focused race tests passed for Kafka, Redis, and
    integration-tagged NATS. Verified `go test ./eventbus/... -count=1`,
    `go vet ./eventbus/...`, and integration-tag compilation on 2026-10-19.
    Live Kafka was not required for the deterministic head-of-line regression;
    production-scale scheduler fault/soak evidence remains assigned to P6.7.6.

- [x] **P6.7.4 Harden envelope, retry, error, and DLQ boundaries**
  - Validate every decoded v3 envelope before handler invocation: nonempty and
    bounded ID, exact topology kind/type match, attempts, timestamps, headers,
    and required metadata (EB-06). Route mismatches go to DLQ with stable,
    sanitized reasons.
  - Make public outcome error types nil-safe; safely render arbitrary handler
    errors; recover panics from caller-provided extension points such as retry
    policies without exposing panic values (EB-07).
  - Reject or clamp negative, overflowing, NaN-equivalent, and otherwise
    pathological retry delays from `RetryPolicy` and `MarkRetryAfter`.
  - Bound and sanitize reason/error metadata before logs, traces, and DLQ
    wrappers. Enforce a final encoded DLQ record limit so poison metadata cannot
    cause infinite settlement failure or leak credentials/personal data.
  - Acceptance: hostile error implementations, malformed envelopes, oversized
    error strings, wrong route metadata, and invalid retry delays have
    deterministic non-panicking tests across the core and applicable drivers.
  - Notes: Added route-aware v3 envelope validation before tracing or handler
    invocation. Nonempty bounded ID, exact topology kind/name, positive attempts,
    sane creation/schedule timestamps, valid JSON payload, bounded ordering /
    dedupe metadata, and bounded control-free headers are required. Stable DLQ
    reasons distinguish invalid IDs, route mismatches, attempts, timestamps,
    payloads, dedupe, keys, and headers; the engine no longer overwrites a
    mismatched kind. Legacy v2 decode remains compatible with its missing
    creation timestamp. Public `PermanentError`, `RetryAfterError`, and
    `SkipError` now have nil-safe fixed messages that do not expose caller text.
    Error inspection and `errors.Is`/`errors.As` traversal are panic-isolated,
    and operational logs use fixed codes or error types rather than rendering
    hostile errors. Custom `RetryPolicy.NextBackoff` is recovered; negative
    delays clamp to zero and delays above 30 days clamp to 30 days, including
    `MarkRetryAfter`. Added `transport.Decision.ErrorCode` plus shared metadata
    normalization: reason/error codes allow only `[a-z0-9_.-]`, are bounded to
    64 bytes, never render `Cause`, and default safely for direct transport
    callers. Memory, Redis, NATS, and Kafka enforce a 2 MiB final encoded DLQ
    record bound before source removal/settlement. Deterministic hostile tests
    cover wrong routes, malformed metadata, panic-capable error implementations,
    retry-policy panic, pathological delays, metadata sanitization, and encoded
    DLQ limits across core and all built-in drivers. Updated migration,
    capability, observability, and testing guidance. Verified default EventBus
    tests/vet, integration-tag compilation, the complete embedded-NATS suite,
    and focused race tests for core, Memory, Redis, Kafka, and integration-tagged
    NATS on 2026-10-19. Production-scale poison/DLQ fault execution remains part
    of P6.7.6 rather than being claimed here.

- [x] **P6.7.5 Complete administration and Kafka topology reconciliation**
  - Add kind-explicit task/event statistics and stable Bus-level DLQ
    list/count/requeue/purge APIs, including cursor pagination where supported
    (EB-10). Preserve `ErrNotSupported` and keep destructive actions explicit.
  - Reconcile safety-critical configuration for existing Kafka main, DLQ, and
    delay topics instead of accepting `TopicAlreadyExists` after checking only
    partitions (EB-11). Validate replication, retention, and owned topic
    settings; reject unsafe `cleanup.policy=compact` for queue records.
  - Design an explicit Kafka partition-expansion operation that expands main,
    DLQ, and all delay topics, rejects decreases, reports partial admin success,
    and documents ordering-key remapping. Ordinary startup must not silently
    change partition counts.
  - Serialize or fence Kafka DLQ administration across processes, or provide a
    documented single-operator lease rather than relying only on a process-local
    mutex.
  - Acceptance: public admin examples no longer require access to a concrete
    transport; existing-topic mismatch/repair and partial partition expansion
    have deterministic plus live Kafka tests.
  - Notes: Added kind-explicit `TopologyRef`, `TaskRef`, and `EventRef` plus
    public `Bus.TopologyStats`, `SubscriberStats`, `ListDeadLetters`,
    `CountDeadLetters`, `RequeueDeadLetters`, and `PurgeDeadLetters`. The legacy
    `Stats(name)` remains a task-only adapter. Administration validates declared
    topology/subscriber identity, preserves driver options, supports cursor
    pages where available, and returns stable `ErrNotSupported` without concrete
    transport access. The admin example now uses only `Bus`. Memory, Redis,
    NATS, and Kafka implement explicit subscriber stats. Kafka provisioning now
    creates/reconciles the main, DLQ, three delay, and one-partition DLQ admin
    lock topics; it validates every partition's replication factor, rejects any
    `cleanup.policy` containing `compact`, and alters only caller-declared topic
    configs such as retention. Ordinary startup still rejects partition mismatch
    and never expands topics. Added explicit `ExpandPartitions`, which expands
    main/DLQ/delay topics in stable order, rejects decreases, and returns
    per-topic success plus `PartitionExpansionError` after partial broker
    success; ordering-key remapping is documented. Kafka DLQ requeue/purge now
    acquire a broker-backed consumer-group lease on the topology's single
    admin-lock partition in addition to the local mutex, so competing processes
    fail busy rather than racing cursor commits. Deterministic tests cover public
    admin routing, compaction/replication/config mismatch and repair, decreases,
    and partial expansion. A live Kafka test at `localhost:9092` verified
    retention reconciliation, expansion from one to two partitions across all
    related data topics, and two-transport DLQ lease exclusion/reacquisition on
    2026-10-19. Verified default EventBus/example tests, integration-tag
    compilation, and focused Kafka execution. `go test ./... -count=1`,
    `go vet ./...`, the complete tagged Kafka suite, focused default race tests,
    and the race-enabled live topology-admin test all passed on 2026-10-19.

- [~] **P6.7.6 Complete contract coverage and production-scale evidence**
  - Expand `transport/contracttest.Run` so every applicable item in the shared
    transport contract checklist has an executable capability-gated scenario
    (EB-12). Map each checklist row to exact tests in `CONTRACT_TESTS.md` and
    mark it complete only after every enabled durable driver passes.
  - Add the combined dedupe fault window, topology-name collision, envelope
    route mismatch, scheduler health, the complete P6.7.1R Redis retention and
    capacity matrix, and Kafka delayed head-of-line regressions discovered by
    the pre-Phase 7 review.
  - Run 100,000- and 1,000,000-message backlogs on disposable broker
    infrastructure with immediate, delayed, 10% retry, and 50% retry workloads
    (EB-13). Include restarts, reconnects, Kafka rebalances, scheduler ownership
    changes, and settlement ambiguity.
  - Run a sustained pre-production soak and record broker versions/config,
    throughput, p50/p95/p99 latency, CPU, RSS/heap, disk, lag, duplicate rate,
    DLQ outcomes, goroutine growth, and cleanup procedure.
  - Acceptance: there is no unexplained loss, resource growth is bounded, every
    advertised guarantee has contract evidence, and unavailable infrastructure
    is recorded as an open gate rather than silently skipped.
  - Notes: IN PROGRESS on 2026-10-19. Expanded the reusable transport suite with
    single-use settlement, deterministic unsettled redelivery hooks, dedicated
    stats transitions, same-name task/event routing isolation, complete DLQ
    purge/idempotent requeue, namespace/kind/topology dedupe isolation,
    publication-reservation ownership states, and bounded payload-free scheduler
    health checks. Memory and miniredis pass by default; embedded JetStream and
    live Kafka contract runs pass. Kafka's documented stats model keeps
    fetched-uncommitted obligations in Pending, and scheduler health permits
    inactive delay tiers with zero counters. Subscriber retry isolation passes
    Memory but remains an explicit open gate for Redis/NATS/Kafka rather than a
    false capability claim. Added a bounded large-run benchmark, periodic
    heap/goroutine/lag samples, explicit delay control, pinned profile-scoped
    Redis 7.4.1/NATS 2.10.22/Kafka 3.8.0 disposable fixtures, guarded PowerShell
    orchestration, and `PRODUCTION_SCALE.md`. A disposable Redis 100,000-message
    immediate 1 KiB/concurrency-100 preload completed with 100,000 unique ACKs,
    zero observed duplicates, final lag zero, 1,527.67 msg/s drain throughput,
    6.18 MB peak Go heap, about 74.8 MB peak process RSS, 108 peak goroutines,
    and project-scoped container/network/volume cleanup. The 1m matrix, other
    driver/workload cells, restart/reconnect/rebalance/ambiguity faults, sustained
    soak, and at that checkpoint the remaining shared Bus-level/fault scenarios
    were still open, so no Phase 6.7 exit criterion was claimed.
    Follow-up review on 2026-10-20 added the reusable public-Bus contract suite
    to all four driver registrations, including exact attempts, retry-after,
    permanent/panic/timeout outcomes, malformed/wrong-route envelopes, graceful
    drain, and subscriber-targeted retry/DLQ-requeue isolation. Redis, NATS, and
    Kafka pass its forced-redelivery case; Memory is explicitly durability-
    inapplicable. The embedded NATS case passed ten consecutive runs. Contract
    buses now register bounded failure-safe
    cleanup immediately after opening. Production tooling now requires topology
    drain plus zero final pending/delayed/in-flight/DLQ/lag state, proves requested
    second-worker/restart actions occurred during active work, terminates the
    benchmark child before broker teardown on failure, separates execution from
    acceptance status, and validates documented comma-delimited matrix CLI input.
    Kafka benchmark topologies now provision and report one partition per
    requested concurrency slot instead of inheriting the three-partition demo
    default.
    Added native Ubuntu Bash production, matrix, and validation scripts with
    argument-safe paths, child-before-broker cleanup, final-state/fault checks,
    and named-volume data sampling; the Bash validation suite passed on
    2026-10-20. Added shared retry/DLQ settlement-failure scenarios that cancel
    before destination confirmation, verify the source remains outstanding,
    then prove redelivery and successful settlement. Memory and Redis passed ten
    focused runs, embedded NATS passed five, and live Kafka passed both focused
    cases. Shared pre-confirmation settlement/source-preservation gates are now
    closed; at that checkpoint shared post-commit ambiguity remained open while
    driver-specific fault tests continued to cover it.
    Follow-up Kafka administration verification on 2026-10-20 ran
    `TestKafkaDLQRequeueOwnershipLossDoesNotCommitAfterSuccessor` and
    `TestKafkaDLQPurgeOwnershipLossDoesNotCommitAfterSuccessor` ten consecutive
    times plus once under the race detector; all passed. Subscriber registry
    private-topic filtering no longer aliases its discovery slice, partition
    expansion now reports missing broker metadata explicitly instead of wrapping
    a nil error, and the Memory/Redis contract redelivery hooks use explicit
    signatures. Added a regression for missing Kafka topic metadata. Full
    repository tests and vet passed after these fixes.
    Gate 1 follow-up added shared capability-gated post-commit ambiguity cases
    for retry and DLQ settlement. Redis injects response loss after the exact
    atomic transfer Lua script and proves one destination with the source already
    ACKed. NATS injects after durable transfer and proves stable transfer IDs
    keep one destination while settlement retry terminates the source. Kafka
    injects after broker-confirmed produce and proves the documented duplicate
    append window remains loss-safe before source commit. Memory is explicitly
    inapplicable because it has no broker response boundary. Redis passed ten
    focused/shared runs, embedded NATS passed ten focused and five full-suite
    runs, live Kafka passed three focused and one full-suite run; all three
    focused scenarios passed under the race detector. The shared post-commit
    ambiguity gate is closed.
    The next contract gate adds
    `repeated_reconnect_joins_sessions_without_goroutine_growth` to the public
    Bus suite. It forces five consecutive consume-session cancellations through
    each real durable driver, proves active session count never exceeds one,
    samples goroutines only after `Consume` returns and joins owned work, bounds
    first-to-last growth, then handles, ACKs, and drains a job after recovery.
    Redis and embedded NATS passed ten focused runs each, live Kafka passed three;
    all three passed race-enabled focused runs and their complete Bus contract
    suites. Memory is durability-inapplicable. The shared reconnect/session-leak
    gate is closed; repeated broker-process restart remains production fault/soak
    evidence.
    The scheduler ownership gate adds
    `multi_instance_scheduler_owner_failure_recovers` to the shared transport
    suite. Redis proves a failed owner leaves the delayed source intact for a
    successor atomic move. NATS proves a successor respects a live KV lease and
    reclaims exactly once after expiry. Kafka proves an owner can fetch then exit
    without commit and a successor group member receives, transfers, and commits
    the same durable source. The suite ACKs every allowed ready copy and requires
    final stats to return to zero. Redis passed twenty focused runs, embedded
    NATS ten, live Kafka five; all passed race-enabled focused runs. The shared
    multi-instance scheduler ownership/recovery gate is closed; production-scale
    ownership changes remain fault/soak evidence.
    The ordering gate adds `advertised_ordering_serializes_same_key` to the
    shared transport suite. It publishes eight records with one ordering key,
    consumes with concurrency and prefetch greater than one, proves no later
    same-key record is admitted before the current record settles, verifies
    payload/key FIFO, and requires final stats to return to zero. Live Kafka
    passed ten focused runs, one race-enabled run, and its complete transport
    contract suite. Memory, Redis, and NATS capability-skip because they do not
    advertise ordering. The contract remains explicitly scoped to one physical
    Kafka topic-partition; main, delay, and subscriber-private topics are
    independent logs. The shared capability-gated ordering gate is closed;
    production rebalance and scale behavior remain fault/soak evidence.
    The final shared contract gate strengthens
    `unsettled_session_cancellation_redelivers` and runs it on every driver.
    The scenario loses a consumer session with one unsettled source, verifies
    the replacement delivery preserves all identity fields exposed by the first
    delivery, ACKs it, and requires final topology stats to return to zero.
    Memory restores session-owned in-flight state, Redis reclaims a stale pending
    entry, NATS expires a short AckWait, and Kafka replays the uncommitted group
    offset. Kafka may retain envelope attempt `1`, so the shared assertion allows
    that documented behavior while still proving redelivery. Memory and Redis
    passed twenty focused runs, embedded NATS ten, and live Kafka five; all four
    passed race-enabled focused runs and their complete transport contract
    suites. Universal shared duplicate/redelivery coverage is closed. Handlers
    remain responsible for idempotency under the at-least-once contract.
    Production evidence tooling now emits explicit preload/measurement phases in
    benchmark and broker JSONL. Preload stops its sampler and resets Go heap,
    CPU, elapsed-time, and peak baselines immediately before resume. Bash and
    PowerShell runners measure the broker named-volume data directory with `du`,
    require measurement and numeric volume samples, require preload samples when
    requested, and record phase/volume counts plus maximum volume bytes in the
    summary. A disposable Redis run exposed and fixed a Bash `0.0` versus `0`
    comparison that mislabeled finite runs and incorrectly applied Kafka second-
    worker acceptance. The corrected Redis 100k immediate/preload, 1 KiB,
    concurrency-100 cell completed 100,000 unique messages with zero duplicates
    and final state, 4,354.52 msg/s measurement throughput, 14 preload samples,
    23 measurement samples, 18 numeric volume samples, and verified cleanup. Its
    83,082,531-byte volume maximum predates allocated-block sampling and is
    historical apparent size. This closes phase-correct evidence collection, not
    the remaining matrix or soak gates.
    Redis 1m immediate/preload evidence then completed on the disposable 7.4.1
    fixture with 1 KiB payloads and concurrency 100: 1,000,000 unique messages,
    zero duplicates and final obligations, 4,451.03 msg/s over 224.67 measurement
    seconds, max pending/lag 1,000,000, peak measurement heap 4,665,016 bytes,
    peak goroutines 107, and verified
    cleanup. The first run exposed an unlabeled broker sample before benchmark
    JSONL existed; broker sampling now defaults to the requested initial phase
    and acceptance rejects every invalid phase or nonnumeric volume sample. The
    clean rerun had 29 preload, 45 measurement, and 61 fully valid broker samples.
    Its 443,364,033-byte maximum predates allocated-block sampling and is
    historical apparent size.
    The Redis immediate/preload 1m cell is closed; other workloads and drivers
    remain open.
    NATS 100k delayed/preload evidence then completed with 1 KiB payloads,
    concurrency 100, requested 5-second delay, and an explicitly recorded
    `nats_delay_batch_size=1000`, the validated driver maximum. It completed
    100,000 unique messages with zero duplicates/final obligations at 1,127.80
    msg/s over 88.67 measurement seconds. The first measurement sample showed
    14,000 completed and 86,000 delayed; sampled maxima were 88,000 delayed and
    12,000 pending. Peak high-frequency heap was 97,473,328 bytes, peak
    goroutines 117, and verified cleanup. Its 396,737,438-byte JetStream value
    predates allocated-block sampling and is historical apparent size. An initial
    diagnostic run inherited demo batch 100 and
    took 987.67 seconds (~101.25 msg/s), exposing a benchmark configuration
    ceiling. The benchmark override is now recorded and validated in result and
    manifest while library/demo defaults remain unchanged. The NATS delayed 100k
    cell is closed.
    Kafka 100k retry50 evidence completed with 1 KiB payloads, concurrency and
    partitions 100, and a second worker joining after five seconds. It completed
    100,000 unique messages with zero duplicates/final obligations and exactly
    150,000 handler invocations. Primary and second workers handled 82,968 and
    67,032 respectively; the second joined at 11,542 produced/10,643 completed,
    proving active group participation. Throughput was 4,131.56 unique msg/s over
    24.20 seconds, peak heap 7,829,568 bytes, peak goroutines 302, max pending/lag
    898, and allocated Kafka data peaked at 104,665,088 bytes across 11 samples.
    The Kafka healthcheck now uses local TCP readiness instead of following the
    host-advertised listener from inside the container; startup failures retain
    broker logs. Rebalance acceptance requires nonzero second-worker handling.
    Sparse Kafka segments also caused apparent `du -sb` usage of 11.68 GB; all
    runners now record allocated `du -sk` blocks under explicit schema fields.
    Earlier Redis/NATS volume values therefore require reruns before allocated-
    disk claims. The selected Kafka cell is closed.
    Redis 100k retry50 broker-restart evidence then completed with 1 KiB payloads,
    concurrency 100, a restart after five seconds, and explicitly recorded
    `redis_delay_batch_size=1000`. The restart occurred during active work at
    10,799 produced/9,899 completed with lag 900. The run completed 100,000
    unique messages and exactly 150,000 handler invocations with zero duplicates
    and final obligations at 1,722.95 unique msg/s over 58.04 seconds. Peak heap
    was 4,088,872 bytes, peak goroutines were 107, and allocated Redis data peaked
    at 74,121,216 bytes across 19 samples; cleanup was verified. An earlier
    correctness-pass run inherited demo batch 100 and took 501.07 seconds. The
    benchmark-only Redis override is now recorded and validated in result and
    manifest while library/demo defaults remain unchanged. This closes one Redis
    broker-restart fault cell, not the remaining fault matrix or sustained soak.
    NATS 100k retry50 broker-restart evidence then completed with 1 KiB payloads,
    concurrency 100, a restart after five seconds, and
    `nats_delay_batch_size=1000`. The restart occurred during active work at
    10,799 produced/9,899 completed with lag 2. The run completed 100,000 unique
    messages, all 50,000 expected synthetic retry failures, and exactly 150,000
    handler invocations with zero duplicates/final obligations at 1,733.06 unique
    msg/s over 57.70 seconds. Peak heap was 38,894,840 bytes, peak goroutines were
    124, and allocated JetStream data peaked at 431,722,496 bytes across 19
    samples; cleanup was verified. The first restart run exposed a JetStream
    publish request retaining the whole run deadline. Benchmark enqueue attempts
    are now bounded to five seconds under the parent context. A diagnostic also
    demonstrated that restart redelivery can make a retry candidate first appear
    at broker attempt two, so result and runner acceptance now partition expected
    retries between executed synthetic failures and first-seen-after-attempt-one
    candidates and require exact total handler accounting. This closes one NATS
    broker-restart fault cell, not the Kafka fault cell or sustained soak.
    Kafka 100k retry50 broker-restart evidence then completed with 1 KiB payloads,
    concurrency and partitions 100. The active-only restart occurred at 1,573
    produced/682 completed with lag 902. The run completed 100,000 unique
    messages; all 50,000 unique retry candidates first failed at attempt one,
    while restart replay produced 50,010 synthetic failure invocations. Four
    duplicate completions occurred inside the documented at-least-once ambiguity
    window, and total handler accounting was exact at 150,014 invocations. Final
    pending, delayed, in-flight, DLQ, and lag were zero. Throughput was 2,140.44
    unique msg/s over 46.72 seconds, peak heap 8,809,672 bytes, peak goroutines
    154, max pending/lag 906, and allocated Kafka data peaked at 112,594,944 bytes
    across 17 samples; cleanup was verified. An initial run restarted during
    topology provisioning before any message existed. Timed restart is now an
    earliest eligible action and waits for `produced > 0` plus active production
    or backlog. Retry accounting now separates unique retry candidates from
    repeated failure invocations. This closes the selected Kafka restart cell,
    not the remaining matrix or sustained soak.
    A qualifying two-hour Kafka sustained soak then completed with retry10,
    1 KiB payloads, target 100 msg/s, concurrency/partitions 100, a second worker
    joining after 15 minutes, and broker restarts after 30/60/90 minutes. It
    produced and uniquely completed 711,450 messages, executed all 71,145 expected
    retry failures, observed zero duplicates, and drained every final obligation.
    Primary/second workers handled 440,636/341,959 invocations. Throughput was
    98.81 unique msg/s over 7,200.02 seconds with p50/p95/p99 latency
    1.29/2.83/4.21 ms. Thirty-minute average heap remained 9.52-10.16 MB, window
    maxima 12.74-13.12 MB, and post-drain heap 8.73 MB. Goroutine averages stayed
    near 282-289 after rebalance, with 69 after drain; launcher RSS averages rose
    only from 31.3 to 35.8 MB. Kafka page cache reset across restarts and plateaued
    around 1 GiB. Allocated disk peaked/finalized at 355,037,184 bytes, max lag was
    900, and cleanup was verified. The runner now supports fail-closed repeated
    restart cadence and records `broker_restart_count`. Duration pacing does not
    catch up slots missed during outage, so actual produced versus unique completed
    is the loss invariant. This closes the sustained soak evidence cell, not the
    remaining 100k/1m matrix dimensions.
    Go-focused verification passed on Linux; PowerShell execution remains to be
    rerun on Windows because this environment has no PowerShell executable. The
    remaining 1m matrix cells, durable workload cells, production ordering/rebalance
    and scheduler ownership changes, repeated broker-process restart evidence,
    full matrix volume/resource review, and sustained soak remain open, so the
    task stays `[~]`.

- [ ] **P6.7.7 Finalize the API and scalability model before adapters**
  - Make all public operations return deterministic lifecycle errors while
    preserving the documented producer-only flow before `Start` (EB-14).
  - Separate declaration/provisioning options from subscriber/runtime options;
    reject ignored driver options, conflicting redeclarations, and duplicate
    subscriber replacement (EB-15). Plan removal of implicit event declaration.
  - Add result-returning enqueue/publish APIs exposing stable accepted message
    IDs and durable-prefix batch results while retaining error-only compatibility
    adapters (EB-16).
  - Separate the local concurrency/admission key from the broker partition or
    ordering key so keyed concurrency is not accidentally Kafka-only (EB-17).
  - Make delay infrastructure opt-in or share scheduler resources where the
    broker contract permits; document topology × tier × partition/client costs
    and enforce practical bounds (EB-18).
  - Add an explicit per-topology Kafka capacity model to examples. Do not use
    one global helper that demonstrates concurrency 4 with only 3 partitions;
    document `ceil(partitions / planned instances)` as a starting point, not a
    universal rule (EB-19).
  - Replace the two-field capability descriptor with structured durability,
    ordering scope, settlement guarantee, dedupe, heartbeat, admin, pause,
    payload, and scheduler-health declarations; keep code, docs, dashboards,
    and tests synchronized (EB-20).
  - Acceptance: the API needed by P7.1-P7.3 is stable, compiling migration
    examples cover old and new entry points, and no Phase 7 adapter must depend
    on an ambiguous or silently ignored option.
  - Notes:

### Phase 6.7 exit criteria

- [ ] All P0 and P1 findings in `PRE_PHASE7_REVIEW.md` are closed with
  regression tests; any deferred P2 finding has an explicit compatibility and
  release decision.
- [ ] Successful publication on a durable driver cannot be reproduced without
  durable acceptance, and all physical topology/group names are collision-free
  under supported inputs.
- [ ] Memory has no silent-success loss path, uses bounded queues and delayed
  scheduling, provisions event groups before publish, and passes every shared
  contract scenario applicable to its declared process-local capabilities.
- [ ] Durable schedulers meet their documented deadline/lifecycle guarantees and
  expose actionable failures.
- [ ] Public task/event stats and DLQ administration work without concrete
  transport access.
- [ ] Redis provisions intended groups before compaction, reclaims only below a
  group-safe watermark, applies observable admission backpressure when an
  offline group blocks retention, and demonstrates bounded memory after ACK.
- [ ] Every applicable shared transport contract row maps to passing tests for
  Memory, Redis, NATS, and Kafka, capability-gating durable-only behavior.
- [ ] Disposable 100k/1m backlog runs and a sustained pre-production soak are
  recorded with no unexplained loss and bounded resources.
- [ ] Phase 7-facing publication identities, options, capabilities, and
  lifecycle semantics are finalized and documented.

## 14. Phase 7 - Migration and legacy deprecation

Dependencies: Phase 6.7 complete.

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

## 15. Shared transport contract test suite

Every durable driver must pass the same behavioral tests where its capability matrix says supported:

- [x] enqueue, handle, and ACK
- [x] handler failure and exact attempt bound
- [x] explicit retry-after
- [x] permanent error to DLQ
- [x] decode error to DLQ
- [x] panic recovery
- [x] handler timeout
- [x] settlement timeout/failure
- [x] retry publish failure does not lose source
- [x] DLQ publish failure does not lose source
- [x] duplicate delivery
- [x] delayed execution precision
- [x] multi-instance delay scheduler
- [x] dedupe duplicate, expiry, namespace, and topology isolation
- [x] graceful drain with in-flight work
- [x] forced shutdown and later redelivery
- [x] consumer reconnect without goroutine leak
- [x] task/event routing isolation
- [x] subscriber retry isolation
- [x] ordering guarantee when advertised
- [x] stats accuracy
- [x] DLQ list/requeue/purge/count
- [x] malformed transport message handling

Use channels, fake clocks, and observable hooks instead of fixed sleeps wherever possible.

## 16. Verification commands

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

## 17. Production release gates

All gates are mandatory:

- [ ] Phase 6.7 exit criteria are complete.
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

## 18. Current verification snapshot

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

### Phase 5 completion snapshot (2026-10-18)

- Kafka uses one immutable client factory across producer, grouped/manual
  consumers, delay workers, direct readers, and administration; TLS/SASL and
  timeout settings survive both supported constructors and the legacy adapter.
- Main delivery is bounded and partition-serialized. Session-owned synchronous
  commits advance only through contiguous settled offsets, and rebalance drains,
  rewinds undispatched records, and fences revoked ownership.
- Retry, DLQ, and delay transfer use confirmed publish-before-commit. Fault-window
  tests demonstrate the documented duplicate windows without silent source loss;
  unsafe unacknowledged and transactional producers are rejected.
- Durable bounded delay tiers preserve opaque payloads, keys, and public headers.
  Versioned DLQ administration and offset-derived statistics expose real broker
  state with append-only-log limitations documented for operators.
- Kafka passes every applicable shared transport contract scenario against the
  live broker. Unit repetition, the complete tagged Kafka suite, integration-tag
  compilation, focused race tests, and default/full-repository tests and vet pass.

# EventBus transport contract coverage

The reusable driver boundary suite lives in `transport/contracttest`. It checks
durable publish/consume/ACK, single-use settlement, deterministic unsettled
redelivery where the harness can advance ownership, retry transfer, complete
DLQ administration, delayed delivery, event fan-out and task/event routing,
dedupe scope/publication state, scheduler health, statistics, and malformed
payload settlement. Memory runs every applicable
scenario hermetically against its bounded process-local state. Redis runs the
same suite against miniredis by default and against live Redis 7 single-node and Cluster fixtures
under the `integration` build tag. NATS runs it against an embedded, file-backed
JetStream server under the same tag; its scheduler is stopped by the harness so
due-time dispatch remains deterministic. Kafka runs every applicable scenario
against `localhost:9092`; its tier workers are likewise stopped so the harness
can advance due time deterministically. Capability-dependent scenarios are
skipped only when the transport does not advertise that optional interface, as
with Kafka deduplication.

Core runtime behavior is transport-independent and remains covered by the root
EventBus tests. The second reusable suite, `eventbus/contracttest`, runs public
Bus behavior through each registered transport. A durable driver is considered
contract-complete only when both shared suites and the applicable core tests
pass.

| Required behavior | Coverage | P6.7.6 status |
| --- | --- | --- |
| Enqueue, handle, ACK | `contracttest/publish_consume_ack`; `TestEngine_C4_StateMachine` | Shared boundary passes Memory, Redis, NATS, and Kafka |
| Handler failure and exact attempt bound | `eventbus/contracttest/handler_retry_exact_max_attempts_and_final_dlq`; `TestProcessDeliveryAttemptBounds` | Shared Bus case passes Memory, Redis, NATS, and Kafka |
| Explicit retry-after | `TestBug_C6_DelayUnder1Second`; `contracttest/retry_transfer` | Shared transfer passes all drivers |
| Permanent/decode failure to DLQ | `eventbus/contracttest/permanent_error_directly_to_dlq`; `eventbus/contracttest/malformed_envelope_suppresses_handler_and_has_stable_dlq_reason`; `contracttest/dead_letter_admin` | Shared Bus cases pass all four drivers |
| Panic and timeout | `eventbus/contracttest/panic_recovery_and_worker_survival`; `eventbus/contracttest/handler_timeout_retries_then_dlq` | Shared Bus cases pass all four drivers |
| Settlement timeout/failure | `contracttest/failed_retry_settlement_preserves_source`; `contracttest/failed_dlq_settlement_preserves_source`; `TestEngineRecordsMetricsOnlyAfterSettlementSuccess` | Shared canceled-settlement boundary passes all four drivers |
| Retry/DLQ failure cannot lose source | `contracttest/failed_retry_settlement_preserves_source`; `contracttest/failed_dlq_settlement_preserves_source`; `contracttest/post_commit_ambiguous_retry_is_loss_safe`; `contracttest/post_commit_ambiguous_dlq_is_loss_safe` | Shared pre-confirmation failure passes all four drivers; shared post-commit ambiguity passes Redis, NATS, and Kafka, with Memory capability-inapplicable |
| Duplicate delivery | `contracttest/unsettled_session_cancellation_redelivers`; `TestReclaimPreservesSourceAndDeliveryHistory`; `TestNATSAckWaitExpiryRedeliversWithAttempt`; idempotent handlers remain required | Shared session-loss redelivery passes Memory, Redis, NATS, and Kafka; Kafka may retain envelope attempt while replaying an uncommitted source offset |
| Delay precision and multi-instance scheduler | `contracttest/delayed_delivery`; `contracttest/multi_instance_scheduler_owner_failure_recovers`; `TestDelayedDispatchMultipleSweepers`; `TestDelaySchedulerCASAcrossInstances` | Shared owner-failure recovery passes Redis, NATS, and Kafka; Memory is distributed-ownership-inapplicable |
| Dedupe expiry and isolation | `contracttest/dedupe_scope_expiry_release`; `contracttest/publication_deduper_state`; `TestScopedDedupeIsolationExpiryAndRelease` | Namespace/kind/topology and ownership states pass Memory, Redis, and NATS; Kafka does not advertise dedupe |
| Graceful/forced drain | `eventbus/contracttest/graceful_drain_waits_and_acks_in_flight`; `eventbus/contracttest/forced_deadline_redelivers_unsettled_source` | Graceful drain passes all four drivers; forced redelivery passes Redis, NATS, and Kafka, and is durability-inapplicable to Memory |
| Reconnect without session leak | `eventbus/contracttest/repeated_reconnect_joins_sessions_without_goroutine_growth`; `TestSupervisorReconnectAndCancellation`; `TestSupervisorJoinsSessionBeforeReconnect`; `TestNATSDisconnectReconnectResumesConsumer` | Shared repeated-session case passes Redis, NATS, and Kafka; Memory is durability-inapplicable |
| Task/event and subscriber isolation | `contracttest/event_fanout_groups`; `contracttest/task_event_same_name_routing_isolation`; `contracttest/event_subscriber_retry_isolation`; `contracttest/event_subscriber_dlq_requeue_isolation` | Task/event plus subscriber retry/DLQ-requeue isolation pass all four drivers |
| Ordering when advertised | `contracttest/advertised_ordering_serializes_same_key`; Kafka deterministic/live contiguous-offset and rebalance tests | Shared same-key physical-partition FIFO passes Kafka; Memory, Redis, and NATS do not advertise ordering |
| Stats accuracy | `contracttest/stats_transitions`; other `contracttest` stats assertions; `TestRedisStats*`; `TestNATSStatsUseConsumerObligations` | Shared transitions pass all drivers; Kafka's documented `Pending` includes fetched-uncommitted obligations |
| DLQ list/requeue/purge/count | `contracttest/dead_letter_admin`; `TestRedisDLQ*`; `TestNATSDLQAdminPreservesMetadataAndRequeues` | Shared case now covers duplicate/missing requeue, purge, and count for all drivers |
| Malformed transport message | `contracttest/malformed_payload_remains_settleable`; `eventbus/contracttest/malformed_envelope_suppresses_handler_and_has_stable_dlq_reason`; `eventbus/contracttest/wrong_route_envelope_suppresses_handler_and_has_stable_dlq_reason` | Shared transport and Bus cases pass all four drivers |

`contracttest/double_settlement_rejected` now proves the terminal settlement
guard across every driver. `contracttest/scheduler_health_presence_and_sanitization`
passes Redis, NATS, and Kafka and checks bounded payload-free status. These are
cross-cutting assertions used by the settlement and scheduler checklist rows.

## P6.7.6 contract gate status

All currently listed shared contract rows have applicable executable coverage.
Broker-process restart, rebalance, and scale/soak runs remain production evidence
rather than deterministic shared contract scenarios.

Memory-specific parity coverage adds `TestFanoutCapacityFailureIsAtomic`,
`TestConcurrentPublishRespectsCapacity`,
`TestEventPublishBeforeConsumeReachesProvisionedGroups`,
`TestDelayedSchedulerCapacityOrderAndClose`,
`TestDelayedSchedulerAutomaticallyDispatchesAndReportsBackpressure`,
`TestCancellationRedeliversUnsettledMessage`,
`TestEventRetryReturnsOnlyToSourceGroup`,
`TestEventDLQRequeueReturnsOnlyToSourceGroup`,
`TestDLQCapacityFailurePreservesSourceAndRequeueIsAtomic`,
`TestStatsReportQueuedInFlightDelayedAndDLQ`, and
`TestTopologyScopedDedupeIsolation`. Broker restart, distributed ownership, and
cross-process ordering do not apply because Memory explicitly advertises no
durability or distributed capability.

Redis-specific fault coverage tests both sides of ambiguous network responses:

- failure before command execution leaves publish unaccepted and ACK pending;
- response loss after `XADD` leaves the accepted source durable;
- response loss after atomic retry/DLQ settlement leaves exactly one destination
  and a retry observes the already-settled source;
- response loss after ownership-checked ACK cannot restore or steal the source.

These deterministic command-boundary faults validate the safety invariant
without making the hermetic suite depend on process timing. Live Redis fixtures
then verify the same scripts and shared contract on standalone and clustered
Redis. See `TESTING.md` for commands.

NATS fault coverage uses the embedded file-backed JetStream server and broker
metadata directly:

- `AckWait` expiry redelivers an unsettled source with an incremented broker
  attempt;
- `MaxAckPending = concurrency + prefetch` prevents a third delivery until an
  earlier one is synchronously acknowledged;
- a server shutdown and restart on the same store produces observable client
  disconnect/reconnect events, after which the supervised EventBus consumer
  resumes and handles new durable work;
- retry and DLQ destination failures leave the source pending, while injected
  post-transfer failures reuse stable destination identities;
- duplicate immediate publication is constrained by logical message ID, and
  concurrent scheduler instances claim delayed records through KV revision CAS.

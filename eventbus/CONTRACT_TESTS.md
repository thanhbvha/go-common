# EventBus transport contract coverage

The reusable driver boundary suite lives in `transport/contracttest`. It checks
durable publish/consume/ACK, retry transfer, DLQ administration/requeue,
delayed delivery, event fan-out groups, topology-scoped dedupe expiry/release,
statistics, and malformed payload settlement. Memory runs every applicable
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
EventBus tests. A durable driver is considered contract-complete only when both
the shared driver suite and the applicable core tests pass.

| Required behavior | Coverage |
| --- | --- |
| Enqueue, handle, ACK | `contracttest/publish_consume_ack`; `TestEngine_C4_StateMachine` |
| Handler failure and exact attempt bound | `TestProcessDeliveryAttemptBounds` |
| Explicit retry-after | `TestBug_C6_DelayUnder1Second`; `contracttest/retry_transfer` |
| Permanent/decode failure to DLQ | `TestDecodedMetadataReachesRetryAndDLQ`; `TestDecodeFailurePreservesRawSource`; `contracttest/dead_letter_admin`; `contracttest/malformed_payload_remains_settleable` |
| Panic and timeout | `TestPanicRecoveryDefaultsAndAttemptBounds`; `TestEngine_C1_ProcessTimeout` |
| Settlement timeout/failure | `TestEngineRecordsMetricsOnlyAfterSettlementSuccess`; Redis response-loss tests |
| Retry/DLQ failure cannot lose source | `TestAtomicTransferFailureAndRetry`; `TestTransferResponseLossIsIdempotent` |
| Duplicate delivery | `TestReclaimPreservesSourceAndDeliveryHistory`; `TestNATSAckWaitExpiryRedeliversWithAttempt`; idempotent handlers remain required |
| Delay precision and multi-instance scheduler | `contracttest/delayed_delivery`; `TestDelayedDispatchBoundedAndPreservesEnvelopes`; `TestDelayedDispatchMultipleSweepers`; `TestDelaySchedulerCASAcrossInstances` |
| Dedupe expiry and isolation | `contracttest/dedupe_scope_expiry_release`; `TestScopedDedupeIsolationExpiryAndRelease` |
| Graceful/forced drain | `TestDrainPreservesHandlersAndSettlesPrefetch`; `TestDrainDeadlineLeavesSourcesUnsettled` |
| Reconnect without session leak | `TestSupervisorReconnectAndCancellation`; `TestSupervisorJoinsSessionBeforeReconnect`; `TestNATSDisconnectReconnectResumesConsumer` |
| Task/event and subscriber isolation | `TestTaskAndEventPublishUseIsolatedTopologies`; `contracttest/event_fanout_groups` |
| Ordering when advertised | Kafka deterministic/live contiguous-offset and rebalance tests; Redis and NATS advertise no ordering guarantee |
| Stats accuracy | `contracttest` stats assertions; `TestRedisStats*`; `TestNATSStatsUseConsumerObligations` |
| DLQ list/requeue/purge/count | `contracttest/dead_letter_admin`; `TestRedisDLQ*`; `TestNATSDLQAdminPreservesMetadataAndRequeues` |
| Malformed transport message | `contracttest/malformed_payload_remains_settleable`; `TestReclaimSkipsActiveAndMalformedRecordsRemainPending` |

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

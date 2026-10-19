# NATS delayed scheduler

The NATS EventBus driver keeps delayed jobs outside ready JetStream streams, so
future work does not consume subscriber `MaxAckPending` capacity.

## KV layout

Each topology owns `eventbus_delay_<topology>`:

- `meta.event_layout` is the immutable `targeted-v1` migration fence on event
  topologies.
- `meta.cursor` is the durable Unix-second cursor for the oldest live slot.
- `v2.<second>.<run-at-ms>.<schedule-id>` is an independently claimable delayed
  record. Version 3 adds the immutable event subscriber durable target.
- `transfer.<source-derived-id>` is the retry intent written before source ACK.
- `subscriber.v2.<lane-id>` records whether a private subscriber lane is active
  or quarantined.
- `quarantine.<schedule-id>` retains malformed, legacy untargeted, deleted, or
  otherwise incompatible targeted work without blocking its original slot.

Version 2 ordinary delayed records remain broadcast-compatible. Raw P4.2
`<run-at-ms>_<message-id>` records remain compatible for task topologies, but on
event topologies they have no trustworthy ordinary-versus-retry provenance and
are moved to quarantine, never broadcast. Version 2 event retry records without
a target are also quarantined; version 2 records proven ordinary by their
versioned shape remain broadcast-compatible.

## Subscriber isolation

Event declarations must include `nats.WithEventLayoutTargeted()`. The option is
NATS-specific and intentionally unknown to older drivers, so old instances
reject the declaration instead of joining a targeted-retry deployment.
`EnsureTopology` persists and verifies `meta.event_layout=targeted-v1` before it
creates scheduler state or starts a sweeper. Event publish, delayed publish,
subscriber provisioning and consumption, private-lane operations, scheduler
dispatch, statistics, cleanup, and DLQ requeue all verify the broker marker and
fail closed if it is absent or different. Task topology behavior is unchanged.

The shared event stream contains only the broadcast subject. Each event
subscriber owns an immutable private retry lane:

- stream `eventbus_retry_v2_<128-bit-hash>`;
- exact subject `eventbus.retry.v2.<128-bit-hash>`;
- exact durable pull consumer `eventbus_retry_consumer_v2_<128-bit-hash>`.

The hash includes the encoded topology identity and encoded subscriber durable.
Names cannot overlap the former `<main>.target.*` layout, and no wildcard stream
or consumer can receive another subscriber's targeted retry or DLQ requeue.
Provisioning creates the private stream and durable before the subscriber can
process source work. Scheduler and DLQ publications use `Nats-Expected-Stream`,
so a subject cannot be accepted by an unexpected stream.

`CleanupSubscription` is the explicit destructive lifecycle operation. It CAS
marks the lane quarantined before deleting the broadcast and retry consumers.
The private stream and its ready records remain durable and isolated for manual
inspection. Delayed records subsequently encountered by the scheduler move to
KV quarantine. Reusing a quarantined subscriber identity fails explicitly.

## Claim and dispatch

1. Read the delayed KV revision.
2. CAS the scheduler owner and lease deadline.
3. Publish to the broadcast stream or exact private retry stream, preserving the
   logical ID and using `delay:<schedule-id>` as `Nats-Msg-Id`.
4. After broker acknowledgement, delete only the claimed KV revision.

`nats.delay_batch_size` bounds successful publications per pass to 1-1000
(default 100). `nats.delay_lease` defaults to 30 seconds and should exceed the
worst expected publish latency. Independent schedulers may scan the same slot;
KV CAS permits one active owner.

A stale or deleted subscriber does not fail the scheduler pass: its entry moves
create-before-delete into quarantine and unrelated entries continue. The cursor
can advance once the original slot contains no live work.

## Delivery guarantee and duplicate bound

Transfer is publish-before-ACK/delete and therefore at-least-once. Retry intents
close the source-ACK crash window: source redelivery reconstructs the same
scheduled record. Scheduler CAS prevents concurrent publication before lease
expiry.

A broker acknowledgement followed by failure to delete the KV or DLQ source can
still repeat publication. JetStream suppresses the stable `Nats-Msg-Id` only
inside the configured duplicate window. `nats.duplicate_window` makes this bound
explicit on main, DLQ, and private retry streams; it defaults to two minutes and
must be sized above the maximum expected publish-to-source-delete recovery time.
After that bound, duplicate delivery is permitted and consumers must remain
idempotent. The scheduler never deletes its source before confirmed publish.

## Legacy records

P4.2 keys used `<run-at-ms>_<message-id>` and stored only envelope bytes. Each
task pass migrates a bounded number into deterministic v2 keys,
create-before-delete. Event passes quarantine these unknown-provenance keys
create-before-delete. Non-legacy metadata keys are ignored, and old event retry
records without a target fail closed into quarantine.

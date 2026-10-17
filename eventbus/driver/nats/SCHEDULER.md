# NATS delayed scheduler

The NATS EventBus driver keeps delayed jobs outside the main JetStream stream.
They therefore do not consume durable consumer `MaxAckPending` capacity before
they are ready.

## KV layout

Each topology owns its existing `eventbus_delay_<topology>` bucket. P4.3 uses:

- `meta.cursor`: durable Unix-second cursor for the oldest slot that may still
  contain work.
- `v2.<20-digit-second>.<20-digit-run-at-ms>.<schedule-uuid>`: one independently
  claimable record containing version, schedule ID, logical message ID, exact
  deadline, envelope bytes, owner, and lease expiry.
- `transfer.<source-derived-id>`: an idempotent retry intent that remembers the
  first chosen schedule key across a publish-before-source-ACK retry.

The UUID makes two accepted schedules collision-free even when their logical
message ID and deadline are identical. The scheduler streams one second slot
through the NATS KV watcher's bounded channel; it never mirrors all future jobs
into a process-local heap. When a persisted cursor is far behind, a metadata-only
streaming scan finds the oldest live slot and skips empty downtime.

## Claim and dispatch

1. Read the latest KV revision.
2. If no live lease exists, CAS the owner and lease deadline using `Update`.
3. Publish to the main stream using `delay:<schedule-id>` as `Nats-Msg-Id` and
   preserve the logical ID in `X-EventBus-Message-Id`.
4. After the publish acknowledgement, delete only the claimed revision with
   `LastRevision`.

`nats.delay_batch_size` bounds successful publications per pass to 1–1000
(default 100). `nats.delay_lease` is positive and defaults to 30 seconds.
Configure the lease above the worst expected broker publish latency. Independent
schedulers can scan the same slot; KV CAS permits only one active owner.

The guarantee is at-least-once. Failure before publish leaves or releases the
record. Failure after publish but before delete leaves the record for recovery;
the stable broker ID suppresses a repeat while it remains inside JetStream's
duplicate window, but later duplicate delivery is allowed. The scheduler never
deletes a record before a confirmed publish.

Retry settlement creates the transfer intent before the scheduled record. If
the process stops between those writes, the unacknowledged source is redelivered
and reconstructs the same record from the intent. A successful delayed dispatch
removes both the scheduled record and its retry intent. Intent cleanup failure
can leak metadata but does not remove an unpublished job.

## Legacy records

P4.2 keys used `<20-digit-run-at-ms>_<message-id>` and stored only envelope
bytes. Each pass migrates a bounded number of valid legacy records into a
deterministic v2 record, then deletes the legacy revision. The order is
create-before-delete, so interruption can produce a duplicate but cannot lose
the only durable copy. Keep the delay bucket intact during a rolling upgrade.

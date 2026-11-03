# Database writes, outbox and idempotency

Database transactions and EventBus publication are separate durability boundaries.
A successful DB commit followed by a process crash can lose a direct publication;
publishing first can expose an event whose business transaction later rolls back.
Producer dedupe narrows retry duplication; it does not make DB writes atomic with
a broker or handler side effects exactly once.

## SQL recipe

Use the same ORM Client/transaction for a business write, a unique request record
and its outbox row. Define schema constraints in backend-specific migrations:

- A business request table has a unique `(tenant_id, request_id)` and an immutable
  intent hash. A replay with a different intent is a conflict.
- An outbox table has a unique stable event ID, destination/type, versioned payload,
  creation time and dispatch bookkeeping. Generate the ID before the transaction.
- With `client.WithTransaction(ctx, nil, callback)`, use `tx.Context()` and
  `rootRepository.InTransaction(tx)` for every participating managed repository.
  Validate the request, apply the business mutation and insert the outbox row.
  A failure rolls back all three. Use native `tx.DB()` only for trusted SQL that
  preserves the transaction context and equivalent tenant/write policies.
- On a duplicate request constraint, load the committed request in a fresh context,
  compare the intent, and return the stored result. `ErrCommitUnknown` requires
  reconciliation by this business key; do not blindly repeat the mutation.

A separate application worker claims a bounded batch of committed pending rows.
Choose backend-specific claim semantics, for example a short PostgreSQL
`FOR UPDATE SKIP LOCKED` transaction or an atomic claim token with expiry. Do not
hold a database transaction open while waiting on the broker. Publish using the
row's stable ID via `eventbus.WithMessageID(row.ID)`; where supported, also use a
stable dedupe key and a retention window matching the recovery policy.

After confirmed acceptance, mark the row dispatched only when the claim token
still belongs to this worker. A crash after publish and before marking creates a
duplicate on retry; the consumer must handle it. A publication error is possibly
ambiguous: retain/retry the row using the same ID. Define retry/quarantine,
retention, queue-age alerts and payload schema policy in the application.

## MongoDB recipe

The runnable [transaction example](../examples/db_mongodb/transactions.go) already
writes order, stock and an outbox document in one transaction. Database aliases
must share a ClientGroup/client; deployment must support transactions. The
example demonstrates atomic insertion and replay checks; it does not implement a
dispatcher.

Use a unique request/event `_id` generated outside the retryable callback. Inside
`WithTransactionAcross`, use transaction repositories and `tx.Context()` to store
business state plus the outbox document. MongoDB can rerun the callback, so do not
send HTTP requests, publish broker messages or perform other external effects
inside it. A dispatcher can claim documents with atomic `FindOneAndUpdate` and a
unique claim token/lease, then follow the same publish-before-mark protocol.
Change Streams can wake a dispatcher, but are not a replacement for scanning
pending rows and managing durable resume/recovery policy.

## Consumer inbox / business effect

In the consumer's database transaction, insert a unique
`(subscriber, tenant_id, message_id)` inbox record and apply the business change.
Commit both before returning success to EventBus. On duplicate key, inspect the
existing committed record and its intent; acknowledge only a valid replay.
A crash after commit and before ACK then redelivers safely. Expiring inbox records
shortens the duplicate protection window, so align retention with broker replay
and DLQ recovery policy.

An email, payment gateway or other external service cannot join this transaction.
Use that service's stable idempotency key and reconcile ambiguous outcomes, or
write another outbox step. Redis locks and TTL dedupe do not fence irreversible
business effects. There is no reusable outbox dispatcher/inbox framework in
this library; the recipes intentionally leave backend claiming and operational
policy with the application.

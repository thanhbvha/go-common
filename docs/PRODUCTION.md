# Production readiness

This document is the release contract for go-common v2. The repository is a
composable toolkit: readiness is assigned per package and per deployment
profile, never to every adapter as one undifferentiated claim.

## Maturity labels

| Label | Meaning |
|---|---|
| Candidate | Unit, race, static-analysis and package contract gates are implemented. An application must still validate its own configuration and load profile. |
| Integration candidate | Candidate plus disposable real-infrastructure tests in this repository. Promotion still requires the named deployment topology and failure drills. |
| Reference candidate | Used by the reference backend and covered by its lifecycle, outage and soak tests. |

No package is called stable until a v2 stable tag is published and the required
CI gates pass on that revision. A package not used by an application does not
need to be initialized and does not expand that application's production scope.

## Package matrix

| Package | Maturity | Defaults and limits | Ownership and failure contract | Required adoption evidence |
|---|---|---|---|---|
| `foundation` | Reference candidate | Bounded startup/shutdown; starts unready | Owns registered resources and stops in reverse order; startup rollback and worker failures are retained | Exercise process termination and one failed dependency |
| `logger` | Reference candidate | Synchronous; async queue is opt-in and bounded | Root runtime alone shuts down; child sloggers only add fields | Choose overflow policy and test the real sink under pressure |
| `config` | Reference candidate | File/defaults plus explicit environment overlay | Reload validates before publish and retains last-known-good; watcher must be closed | Test every production env-only and secret setting |
| `telemetry` | Reference candidate | TLS, 10% parent-based sampling, no baggage | `New` is local; bootstrap explicitly installs globals; partial initialization rolls back | Validate collector TLS/auth, cardinality and export loss policy |
| `health` | Reference candidate | One in-flight call per checker | A stuck checker makes subsequent readiness probes fail without spawning more calls | Set a timeout below the orchestrator probe budget |
| `xerrors`, `web`, `recovery` | Candidate | Unknown and all 5xx responses are masked | Public messages are separate from diagnostic causes; query strings are excluded from default spans | Adversarial response, panic and proxy-header tests |
| `auth` | Reference candidate | Issuer/audience/lifetime policy and token-size bounds are required | Key IDs are explicit; revocation dependency errors fail closed and map to masked HTTP 503; refresh rotation is atomic with replay detection | Protect keys, namespace Redis, rate-limit login/refresh and test revocation outage |
| `httpclient` | Candidate | Pure HTTP(S) origin/base path; safe methods only retry by default; total budget and response limit are bounded | Resty request engine; transport spans always finish; unsafe retry requires explicit idempotency | Set application timeouts, retry budget and breaker classification |
| `db/orm` | Integration candidate | Admission, query, bulk and pagination limits are configurable | Client owns its pool; transactions reject cross-client use and ambiguous completion is surfaced | PostgreSQL/MySQL version matrix and transaction failure drill |
| `db/mongodb` | Integration candidate | Explicit pool, operation and payload bounds | Manager owns named clients; transaction/change-stream contexts are mandatory | Replica-set/TLS tests and step-down/reconnect drill |
| `db/migration` | Candidate | No startup migration default | Manager owns an instance-local Goose provider but not a borrowed SQL pool | Run as a separately authorized deployment job |
| `redis` | Integration candidate | Single, Cluster and Sentinel; TLS/ACL options | `Close` is terminal; health never creates a new pool; retries honor cancellation | Validate exact topology, ACL/TLS and Sentinel failover |
| `cache` | Candidate | Namespace required; no `FLUSHDB`; lock TTL is at least 1 ms | Each lock acquisition has a one-use ownership session; constructor/session entropy errors are returned, never panicked | Size local admission/counters and test lease loss |
| `ratelimit` | Reference candidate | Whole-millisecond fixed windows; failure policy is mandatory | 429 means exhausted quota; fail-closed backend errors return 503 | Choose fail-open/closed per route and load-test hot keys |
| `cron` | Integration candidate | Local scheduler core; distributed mode uses a lease adapter | Lease uncertainty cancels the job; business effects still need idempotency | Redis failover, process-death and long-running renewal tests |
| `grpc` | Candidate | Starts `NOT_SERVING`; message/deadline limits are explicit | Readiness changes only after dependencies are ready; shutdown is bounded; server/transport error details are masked | Configure TLS, auth and workload-specific message limits |
| `graphql` | Candidate | Body, selection, complexity and active-request limits | Unknown errors are masked; loaders and hosting server retain their own ownership | Schema-specific authorization, fuzz and complexity tests |
| `kafka` | Integration candidate | Convenience consumers are bounded and manually commit successful work | One consumer per subscription; stop on handler/commit failure; producer completion owns copied records | Rebalance, broker restart, poison record and commit-failure drills |
| `nats` | Integration candidate | Context-bound Core and JetStream operations | Client owns connection; drain waits until completion or timeout then closes | JetStream retention/replica policy and reconnect drill |
| `eventbus` | Integration candidate | Bounded workers, payloads, retries and optional DLQ | At-least-once only; transport settlement is explicit; side effects need inbox/idempotency | Run the complete contract/fault matrix for the selected driver |
| `websocket` | Integration candidate | Conservative payload/queue/rate defaults; production validation requires an authorizer | Manager owns connections; network I/O is outside shard locks; tenant scope and public client errors are explicit | Linux/TLS capacity, broker outage and slow-client soak |
| `utils` | Candidate | Small crypto/generic helpers; no application runtime | Callers own keys, entropy and lifecycle decisions | Fuzz serialized inputs and benchmark only used helpers |

## Cross-package rules

1. Use `foundation` as the only process lifecycle coordinator. Do not combine it
   with `utils/graceful` signal handling.
2. Keep package cores independent. SQL/Redis health checkers and Redis cron leases
   live in integration subpackages; packages do not import `foundation` backward.
3. Treat every broker contract as at-least-once. Acknowledgement, offset commit,
   lease renewal and deduplication do not make database side effects exactly-once.
   Use the [outbox/inbox recipes](../db/OUTBOX.md) and stable business keys.
4. Do not put raw errors, credentials, tokens, query strings or unbounded user
   identifiers in public responses, metric labels or span attributes.
5. Migration, broker provisioning and destructive cache administration are
   deployment operations, not request-path conveniences.

## Release gates

A v2 prerelease may be cut after the repository quality, fuzz, database, broker
and reference-service jobs pass on one revision. A stable tag additionally needs:

- no reachable vulnerability without a written disposition;
- the selected real-infrastructure and failure matrix passing without skips;
- a 60-minute reference soak within its fixed error/resource budgets;
- runnable documentation for changed APIs and rollback notes;
- at least one pilot service report for every package promoted beyond Candidate.

See [validation evidence](VALIDATION.md), the [v2 migration guide](V2.md), and
the [reference backend](../examples/production/README.md).

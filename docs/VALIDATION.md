# Validation evidence

This page records reproducible evidence for the current working v2 candidate.
It is not a benchmark promise for another machine or deployment. CI reruns these
gates; a release should update the date and revision after the final commit.

## Gate commands

```bash
bash .github/scripts/check-boundaries.sh
go mod verify
go build ./...
go test -race -count=1 -timeout=180s ./...
go vet ./...
bash .github/scripts/security-gate.sh
python3 -m unittest discover -s .github/scripts -p 'test_*.py'
bash .github/scripts/test-faults.sh
bash .github/scripts/test-fuzz.sh
bash .github/scripts/test-databases.sh
bash .github/scripts/test-messaging.sh
bash .github/scripts/test-mongodb.sh replica
bash .github/scripts/test-mongodb.sh secure
bash .github/scripts/test-brokers.sh
PRODUCTION_SOAK_DURATION=60m bash .github/scripts/test-production.sh
```

Every infrastructure script creates isolated fixtures and cleans up only the
processes or containers it created. Required integration mode rejects missing
fixture addresses rather than silently skipping or using localhost defaults.

## Release evidence workflow

Run `release-evidence` manually on the exact candidate revision. It runs quality,
security, fuzz, SQL, both MongoDB profiles, brokers, repeated fault drills and a
fixed 60-minute reference soak in one checkout. No tag or release is published.
The final verifier rejects missing gates, failed commands, revision mismatches,
modified logs, skipped infrastructure/fault tests and missing soak completion.
Verbose test logs make skips visible; scanner errors also fail the security gate.

Download `release-evidence-<sha>-<attempt>` from that successful trusted Actions
run. Extract it into `artifacts/`, then check it from the repository root with:

```bash
python3 .github/scripts/release-evidence.py verify <full-40-character-sha>
```

Each gate records its command, revision, run/attempt IDs, exit status and log
SHA-256. These hashes detect corruption; they are not signatures. Check the
trusted workflow run's revision and conclusion as well as the local verifier.
Handwritten historical results below do not substitute for this release run.

The security gate retains the JSON scan and checks source/symbol findings before
converting that same stream through govulncheck's text handler. JSON mode alone
does not fail on vulnerabilities. Reachable findings fail closed; module-only
findings remain visible for review and require the disposition below.

Local CI-gate checks on Windows/amd64 with Go 1.27.3 on 2026-11-03 passed:
`go test ./...`, `go vet ./...`, five shuffled race-enabled fault-suite passes,
the evidence verifier regression cases, shell syntax and workflow YAML parsing.
The live security scan found zero reachable/imported vulnerabilities and one
module-only finding. Docker broker/database suites and a fresh 60-minute soak
were not run in this check. This working-tree validation is not release evidence
for a committed revision; the trusted workflow must still run after commit.

## Transactional messaging verification on 2026-11-04

Local transactional messaging verification on 2026-11-04 (Linux/amd64, Go
1.26.10) passed the PostgreSQL storage/transaction/fault/migration suite on
PostgreSQL 16, 17 and 18; the PostgreSQL 18 + Redis/Kafka/NATS matrix; and the
runnable messaging example. Commit acknowledgement loss, lease recovery,
concurrent inbox deduplication, tenant wire identity, consumer retry and DLQ
replay were exercised under `-race`. The repository build, full race suite,
vet, module integrity, boundary checks and release-evidence verifier tests also
passed. These are local working-tree checks, not trusted release evidence for
a committed revision. The messaging CI matrix and release-evidence gate rerun
these scenarios on the candidate revision.

The example-specific matrix was also run on PostgreSQL 16, 17 and 18 with
Redis, embedded real NATS JetStream and Kafka (nine combinations). For each
database version, `MESSAGING_POSTGRES_VERSION=<version> bash
.github/scripts/test-messaging.sh` passed the storage/broker fault suite,
`TestMessagingExample/{redis,nats,kafka}` under `-race`, and the actual example
CLI with `-broker all`, without skipped tests. Each example verified producer
rollback, atomic commit, expired-claim recovery and lost publication response:
three claims, two publish calls, one committed order/business effect and a valid
inbox duplicate replay. The script removed its disposable containers afterward.

MongoDB messaging integration was subsequently verified locally on MongoDB
8.0.16 using the same storage contract and real-broker fault harness as PostgreSQL.
`test-mongodb.sh replica` passed three-node primary election/step-down, standalone
rejection, transaction callback replay, unknown and late commit acknowledgement,
ambiguous claim recovery, cross-database scope, schema/index validation and
explicit cleanup. The secure profile passed the same applicable cases over
TLS/authentication. `test-messaging.sh` passed PostgreSQL and MongoDB with all
Redis/NATS/Kafka broker cases and the PostgreSQL example. All three fixture runs
used `-race`, completed without skips/races, and removed their owned containers.
The full repository race suite, build, vet, module integrity and package-boundary
checks also passed after adding the MongoDB driver. These remain local working-tree
results; the updated MongoDB and messaging release gates must run on the candidate
revision before release.

The runnable messaging example now supports `-driver postgres|mongodb|all`.
Local verification passed all six database/broker example tests and CLI runs
with `-driver all -broker all`. The MongoDB example also passed both replica and
TLS/auth fixture profiles, including standalone rejection in the replica profile.
It verified business/audit database transactions, immutable message conflicts,
exact int64 projection, and an independent transaction callback that ran twice
while committing one projection. The repository race suite and build/vet/boundary
checks passed after the example update; fixture scripts cleaned up their resources.

The messaging example now reads `config.yaml` through the shared config loader,
with existing fixture environment overrides and explicit CLI selection overrides.
Local verification passed the config parsing/precedence/error tests and example
vet checks. The isolated PostgreSQL 18 + MongoDB 8.0.16 fixture script passed under
`-race`, including all six database/broker example tests loading connections from
files without environment overrides, and all six actual CLI runs loading the
sample YAML with isolated environment endpoints. No test skips or race reports
were present in this run; owned containers were cleaned up.

MySQL messaging was subsequently verified on an isolated Oracle MySQL 8.4/InnoDB
fixture using the shared storage contract and Redis/NATS/Kafka fault suite.
Native commit-response loss preserved `orm.ErrCommitUnknown`; regression tests
passed for caller REPEATABLE READ snapshots, CLIENT_FOUND_ROWS inbox ownership,
binary identity comparison, non-UTC DSN/session settings and lease expiry during
lock waits. Migration Up/Down/reapply, schema/index validation and canceled inbox
rollback passed. The full fixture script then passed all nine database/broker
example tests and all nine CLI runs under `-race`, with no skipped tests or race
reports. Repository-wide race tests, build, integration-code vet, boundaries and
module verification passed. CI now includes a MySQL 8.4 messaging job and requires
MySQL messaging evidence; these are local results, not an executed CI run.

## Evidence collected on 2026-11-03

Host: Linux/amd64, Intel Core i7-12700F. Results are local evidence; the final CI
revision remains the release authority.

| Area | Result |
|---|---|
| Reference unit/fault test | Lifecycle, startup readiness, JWT auth, 429 quota, Redis-outage 503, degraded readiness and live liveness passed under `-race` |
| Reference soak | 60 minutes, 719,988 completed requests, 0 failures; goroutines 10 to 6; heap 2,258,024 to 2,777,176 bytes |
| SQL | PostgreSQL 18 and MySQL 8.4 ORM integration/examples passed; transaction, admission, lifecycle, classification and telemetry contracts exercised |
| MongoDB | Replica-set and TLS/auth suites passed for `db/mongodb` and its runnable example |
| Redis | Single/Cluster/Sentinel/TLS/ACL tests passed on isolated fixtures; Sentinel master loss and lease-loss paths are exercised |
| Broker matrix | 16 integration packages passed under `-race`: Redis single/Cluster, Kafka, NATS JetStream, EventBus, auth refresh sessions and WebSocket routing/bridge/cluster |
| Fuzz | All 10 bounded targets passed: password hashes, tokens, ciphertext, config, GraphQL, pagination/cursors and WebSocket envelopes/lifecycle/pubsub |
| Static/runtime | Go 1.26.10: `go mod verify`, boundary checks, build, vet and the full race suite passed; 87 tested packages passed and 54 packages had no test files. Go 1.27.3: repository build passed |

Required integration mode fails on missing fixtures rather than converting an
infrastructure assertion into a skip. Keep that behavior on release revisions.

## Performance snapshots

These short microbenchmarks guide regressions only; they are not capacity claims.

| Operation | Local range |
|---|---|
| synchronous logger, discarded writer | 213-220 ns/op, 0 B/op, 0 allocs/op |
| asynchronous logger admission | 427-434 ns/op, 16 B/op, 1 alloc/op |
| readiness evaluation | 1.69-2.64 us/op, 1,336 B/op, 14 allocs/op |
| WebSocket broadcast to 1,000 in-memory recipients | 62.8-75.9 us/op, about 8.2 KiB/op |
| PostgreSQL cursor benchmark | 296 us/op, 32,373 B/op, 567 allocs/op |

Keep raw logs as CI artifacts for comparisons. Optimize only after profiling the
actual service workload, including network, TLS, broker and database latency.

## Vulnerability disposition

`govulncheck v1.9.0` reported zero package/symbol-reachable findings. It reported
one module-only trace, `GO-2026-5932`, because the graph contains
`golang.org/x/crypto v0.58.0`. The affected imports are the unmaintained
`golang.org/x/crypto/openpgp` family; none is present in the output of
`go list -deps -test ./...`. The scanner's fixed version is a pseudo-version
(`v0.58.1-0.20261009181718-2f1f834330b2`), so this candidate retains the latest
selected stable dependency, excludes OpenPGP, and records the result rather than
silencing it. Re-evaluate and move to a compatible stable fix before release if
one becomes available. The JSON scan is retained as a release artifact.

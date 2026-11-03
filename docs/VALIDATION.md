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
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
bash .github/scripts/test-fuzz.sh
bash .github/scripts/test-databases.sh
bash .github/scripts/test-mongodb.sh replica
bash .github/scripts/test-mongodb.sh secure
bash .github/scripts/test-brokers.sh
PRODUCTION_SOAK_DURATION=60m bash .github/scripts/test-production.sh
```

Every infrastructure script creates isolated fixtures and cleans up only the
processes or containers it created. Required integration mode rejects missing
fixture addresses rather than silently skipping or using localhost defaults.

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

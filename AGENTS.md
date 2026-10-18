# Repository guidance

- Follow [SECURITY.md](SECURITY.md) for all changes; it is also loaded by `thothan.json`.
- This is one library module (`github.com/thanhbvha/go-common`), not a root application. Use Go **1.26.0** as declared in `go.mod`; the root README's Go 1.22+ requirement is stale. Runnable consumers live under `examples/`.

## Wiring and boundaries

- Core packages are framework-independent; HTTP/auth/GraphQL/WebSocket integrations live in framework-specific adapter or middleware subpackages. Keep framework types in those integrations.
- `foundation.App` runs startup hooks sequentially: hooks must return rather than serve/block inline. Its shutdown hooks execute LIFO. `utils/graceful` also has a separate global shutdown registry; do not assume a global registration joins an App instance's registry.
- EventBus wiring starts at `eventbus.Open` -> registry or `WithTransport` -> engine -> `eventbus/transport` -> driver. Keep it distinct from the existing `queue`, `queue_nats`, and `queue_kafka` APIs. Read `eventbus/TESTING.md` before changing its tests.
- Configuration uses Viper through `config.Load`/`LoadEnv`; set `AutomaticEnv` explicitly or use `DefaultOptions()` (a zero-value `Options` does not enable it). Nested dots/dashes map to underscores, with an optional `EnvPrefix`.

## Commands and verification

- Run commands from the module root: `go test ./...` and `go vet ./...`. Focus a package with `go test ./auth`; focus a test with `go test ./auth -run '^TestName$' -count=1`. There is no repository task runner or CI-enforced command sequence.
- `go test ./eventbus/...` must remain broker-free. Opt-in brokers/spikes: `go test -tags=integration ./eventbus/...`; Kafka needs `localhost:9092`, NATS tests start an embedded JetStream server, Redis driver tests use miniredis. Compile the tagged suite without running it with `go test -tags=integration ./eventbus/... -run '^$'`.
- `cache/redis_cache_test.go` connects to `localhost:6379` and skips when unavailable; a passing default suite does not prove Redis cache integration ran. ORM tests use in-memory SQLite.
- In restricted Windows sessions, use the ignored local cache: PowerShell `$env:GOCACHE="$PWD\.gocache-review"`, or Bash `GOCACHE="D:/Go/go-common/.gocache-review" go test ./eventbus/...`.
- GraphQL example codegen must run from `examples/graphql_web`: `go run github.com/99designs/gqlgen generate`. Edit its schema/config/resolvers, not `graph/generated.go` or `graph/model/models_gen.go`. `tools_test.go` pins gqlgen behind the `tools` tag; it is not a normal test suite.
- Run an example as a package, e.g. `go run ./examples/kafka/`; check that example's README and configuration for external services first.

<!-- graft:start -->
## Graft — repo context graph

- This repo is graft-indexed. Get code context from the graph before raw searches or source reads; start with `graft map` for orientation. Results are navigation aids: verify relevant source before editing.
- `graft ask "<question>" --source` — default for finding or understanding code; ranked results include source spans. Read the exact source range when a result is truncated or lacks needed detail.
- `graft grep "<literal>" --fixed` — every literal occurrence in indexed files, rather than ranked top-N results; use ordinary search for unindexed files.
- `graft skeleton <file>` — the file's API signatures and spans without reading all bodies.
- `graft callers <symbol>` — indexed callers; add `--depth all` for the transitive blast radius before refactoring, or `--direction out` for callees. Structural edges are best-effort, not compiler-verified.
- `graft build` — refresh after large changes or create the graph on a fresh clone; structural indexing needs no API key or model. After completing and verifying each implementation task or plan section, run `graft build` before reporting completion so the graph includes the latest mappings. If the rebuild fails, report the failure explicitly rather than silently relying on stale graph data. `graft/` is an ignored, regenerable local cache: never commit or hand-edit it.
- Windows setup: graft is installed through Scoop's Node.js LTS under `%USERPROFILE%\scoop\apps\nodejs-lts\current\bin`. Reopen the terminal after installation to refresh PATH. Native dependency installation needs real Python and Visual C++ Build Tools; Windows Store Python aliases do not suffice.
<!-- graft:end -->

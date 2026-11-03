# Production reference service

This example wires the recommended backend path rather than hiding it behind a
framework: Foundation owns lifecycle, slog owns structured logging, telemetry is
installed explicitly, PostgreSQL and Redis drive readiness, JWT authentication
protects the API, and the rate limiter fails closed.

It intentionally does **not** run database migrations, expose a token-minting
HTTP route, trust forwarded client IP headers, or assume exactly-once effects.

## Configuration

All values use the `APP_` prefix. File-backed settings may be added through the
config package, but environment-only values are decoded and validated the same
way.

| Variable | Required/default | Purpose |
|---|---|---|
| `APP_ADDRESS` | `127.0.0.1:8080` | HTTP listen address |
| `APP_POSTGRES_DSN` | required | PostgreSQL DSN; schema is provisioned separately |
| `APP_REDIS_ADDRESS` | `127.0.0.1:6379` | Dedicated Redis endpoint |
| `APP_REDIS_PASSWORD` | empty locally | Redis password; use ACL/TLS in the real deployment |
| `APP_AUTH_SECRET` | required, at least 32 random characters | Demo HMAC key; production secret management owns rotation |
| `APP_OTLP_ENDPOINT` | empty/disables export | OTLP gRPC endpoint |
| `APP_OTLP_INSECURE` | `false` | Explicit local-only plaintext exporter switch |
| `APP_RATE_LIMIT` | `100` | Requests per one-second fixed window per authenticated subject |

## Run locally

Provision PostgreSQL and Redis first, then run:

```bash
export APP_POSTGRES_DSN='host=127.0.0.1 port=5432 user=postgres dbname=reference sslmode=disable'
export APP_REDIS_ADDRESS='127.0.0.1:6379'
export APP_AUTH_SECRET='replace-with-at-least-32-random-characters'
go run ./examples/production
```

For a local synthetic token, use the CLI path with the same auth settings:

```bash
go run ./examples/production -mint-token user-123
```

The service exposes `GET /live`, `GET /ready`, and authenticated
`GET /v1/me`. Redis failure makes quota checks return 503 and readiness fail;
liveness remains available. Exhausted quota returns 429.

## Verification

```bash
go test -race ./examples/production
PRODUCTION_SOAK_DURATION=60m bash .github/scripts/test-production.sh
```

The soak script creates dedicated PostgreSQL and Redis containers and removes
only those containers. Adapt the reference into an application only after
choosing TLS, secret rotation, migration, tenancy, request budgets and workload
limits. See the [production matrix](../../docs/PRODUCTION.md).

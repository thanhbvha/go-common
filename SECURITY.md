# Security rules for agents

These rules are binding for every agent changing this repository, including examples and tests.

## Secrets and configuration

- Never commit secrets, API keys, tokens, or credentials. Read deployment secrets from the environment. Check staged changes for them before every commit.
- Never print secrets, tokens, or personal data to logs, errors, terminal output, or telemetry. Use synthetic test values; redact credentials in URLs, DSNs, headers, and payloads.
- Configuration is supplied by library consumers through `config.Load`/`LoadEnv` and client option structs; there is no central application secret store. `LoadEnv` can read a local `.env`; enable `AutomaticEnv` explicitly with `Load` or use `DefaultOptions()`. Root `.gitignore` does not exclude `.env`: ensure any new local secret file is ignored before creating it.
- Treat hard-coded credentials and token-printing patterns in examples as demonstrations, not deployment defaults. Do not substitute real credentials into examples, Docker configuration, or tests.
- `httpclient/telemetry.go` records the request URL. Do not put credentials, tokens, or personal data in URLs; redact sensitive URL components before adding observability that could export them.

## Trust boundaries and data access

- Validate and sanitize request bodies, query parameters, headers, uploads, webhook payloads, broker messages, and third-party API responses before use. Apply size and resource limits before expensive decoding or processing.
- Reuse `web/validator.Struct` and its registered rules for struct validation where appropriate. Decoding JSON or a broker payload is not validation; queue/EventBus handlers must validate their own payloads and authority.
- Use parameterized queries or the repository's GORM query builder. Never build SQL, shell commands, or file paths by concatenating untrusted input; constrain file access to an intended base directory.
- `db/orm.Repository` accepts raw conditions and SQL, and `PageRequest.Sorts` is passed directly to GORM `Order`. Keep SQL fragments trusted, bind values separately, and allowlist client-selectable column names and sort directions. Parse and validate IDs before passing them to GORM's ID overloads.
- MongoDB filters, operators, update documents, and aggregation pipelines must be constructed server-side from validated inputs; do not pass arbitrary client-supplied query documents through.
- Repositories provide no automatic tenant or ownership authorization. Add trusted tenant/owner filters to reads and writes; an object ID alone is not permission. Use `WithUnscoped` and restore/delete operations only on authorized paths.
- Escape or encode output for its rendering context. Use JSON encoders and `html/template` for the corresponding boundaries; never introduce raw HTML injection, evaluation, or dynamic code/module loading from untrusted input.

## Authentication and exposed services

- Check authentication and authorization server-side on every privileged HTTP route, GraphQL resolver, RPC, WebSocket action, and background operation. Never trust a client-supplied identity, role, or object ID.
- Reuse `auth` managers for token verification and framework-specific `auth/middleware/*` for protected routes. Middleware stores verified `UserInfo` under `utils/ctxkey.UserInfo`; it does not enforce roles, ownership, or tenancy.
- Preserve signature/algorithm and time-claim validation, signing-key length checks (at least 32 bytes), and encrypted-token AES key checks (exactly 32 bytes). When using token binding, supply the same trusted session/device AAD at issuance and validation; do not silently drop it or replace it with arbitrary request data.
- Generic auth managers expose refresh tokens and revocation separately from JWT verification. `VerifyToken` does not call `IsRevoked`; wire revocation checks explicitly when required and propagate Redis failures. Do not treat an absent Redis client as revocation enforcement.
- Refresh-token Redis keys use `auth:rt:` plus user ID, and blacklist keys use `auth:bl:` plus token ID. Managers sharing Redis share these key spaces: namespace identities or isolate storage when integrating separate tenants/applications. Authenticate the user before refresh/revoke operations.
- WebSocket adapters' fallback `Authenticate` callbacks return the raw `token` query parameter as identity without verifying it. Supply a real authenticator for protected endpoints. Gin/Echo default upgraders accept every origin; configure an origin allowlist for browser-facing connections. Keep rate/connection limits and protect stats/shard-management endpoints.
- `grpc.NewServer` installs recovery/logging/error/telemetry plumbing, not authentication or TLS, and registers reflection. Do not describe its defaults as securing privileged RPCs; explicitly secure service integration and reflection exposure.
- Preserve TLS certificate verification and broker/database authentication. Local plaintext/insecure example settings must not become production defaults.

## Outbound requests and cryptography

- Guard server-side fetches of user-supplied URLs against SSRF. `httpclient` wraps Resty with retries, circuit breaking, and telemetry; it provides no SSRF guard. Restrict schemes and destinations, reject loopback/private/link-local/metadata addresses, and enforce the policy on resolved connection addresses and every redirect. A hostname check alone is insufficient.
- Use vetted crypto libraries and existing `utils/crypt` helpers; never hand-roll crypto or use fast hashes for passwords. Reuse `HashPasswordArgon2id`/`VerifyPasswordArgon2id` for passwords and authenticated encryption such as AES-GCM or ChaCha20-Poly1305 for new encrypted data. Do not use the available MD5/SHA helpers or unauthenticated AES-CBC as substitutes.
- Use `crypto/rand` for security-sensitive randomness. Preserve nonce uniqueness, authentication-tag checks, and constant-time comparisons. Bound untrusted password-hash parameters before expensive verification.

## Dependencies and controls

- Prefer existing vetted dependencies over adding new ones. Keep `go.mod` and `go.sum` authoritative and consistent; do not bypass module checksum verification. Keep tool versions tied to the module, including the gqlgen tools import.
- Never weaken or disable an existing security control (authentication, authorization, validation, encryption, origin/header policies, limits, or permission prompts) to make something work. Raise the tradeoff to the user instead.

# Auth Examples

This directory contains runnable examples for the policy-driven auth API. One
binary exposes four demos:

| Demo | Purpose | Dependencies |
| --- | --- | --- |
| `standard` | Standard JWT login/profile on net/http, Fiber, Gin, or Echo | JWT secret |
| `encrypted` | AES-256-GCM payload with cookie-bound AAD | JWT secret and AES key |
| `generic` | Custom claims, refresh rotation, access-token revocation | JWT secret and Redis |
| `rotation` | Signing and encryption rotation with `kid`/`ekid` | Two signing and two AES keys |

Every HTTP demo listens on `http://localhost:3000`. The code intentionally uses
the new options-based constructors; deprecated compatibility constructors are
not used.

## Prerequisites

Use randomly generated secrets. HS256 signing keys must contain at least 32
bytes. AES-256 keys must contain exactly 32 bytes.

Bash:

```bash
export AUTH_JWT_SECRET='replace-with-a-random-secret-at-least-32-bytes'
export AUTH_AES_KEY='01234567890123456789012345678901'
```

PowerShell:

```powershell
$env:AUTH_JWT_SECRET='replace-with-a-random-secret-at-least-32-bytes'
$env:AUTH_AES_KEY='01234567890123456789012345678901'
```

The literal AES value above is suitable only for local demonstration. Never
copy it into a deployed application.

## Standard JWT And Framework Adapters

Run one of the supported HTTP stacks:

```bash
go run ./examples/auth -demo=standard -stack=nethttp
go run ./examples/auth -demo=standard -stack=fiber
go run ./examples/auth -demo=standard -stack=gin
go run ./examples/auth -demo=standard -stack=echo
```

Issue and use a token:

```bash
TOKEN=$(curl -fsS -X POST http://localhost:3000/login | jq -r .access_token)
curl -fsS http://localhost:3000/api/profile \
  -H "Authorization: Bearer $TOKEN"
```

The profile response includes both `UserInfo` and validated registered claims.
Middleware stores them through `ctxkey.GetUserInfo` and
`ctxkey.GetAuthClaims`.

## Encrypted JWT With Required AAD

Run the encrypted net/http example:

```bash
go run ./examples/auth -demo=encrypted
```

Login stores a random session binding in an HttpOnly, SameSite=Strict cookie.
Keep that cookie alongside the bearer token:

```bash
curl -fsS -c /tmp/go-common-auth.cookies \
  -X POST http://localhost:3000/login \
  -o /tmp/go-common-auth-login.json

TOKEN=$(jq -r .access_token /tmp/go-common-auth-login.json)
curl -fsS -b /tmp/go-common-auth.cookies \
  http://localhost:3000/api/profile \
  -H "Authorization: Bearer $TOKEN"
```

Changing or omitting the cookie causes authentication to fail because
`auth.RequireAAD` validates the binding before decryption. The localhost cookie
uses `Secure=false`; deployed HTTPS applications must set `Secure=true`.

This format is an application-specific encrypted JWT, not standard JWE. Use a
dedicated JWE implementation when cross-platform interoperability is required.

## Generic Claims, Refresh And Revocation

Start Redis locally, for example:

```bash
docker run --rm -p 6379:6379 redis:7-alpine
export AUTH_REDIS_ADDR='127.0.0.1:6379'
go run ./examples/auth -demo=generic
```

`AUTH_REDIS_PASSWORD` is optional. Login returns an access token and a refresh
token:

```bash
LOGIN=$(curl -fsS -X POST http://localhost:3000/login)
ACCESS_TOKEN=$(printf '%s' "$LOGIN" | jq -r .access_token)
REFRESH_TOKEN=$(printf '%s' "$LOGIN" | jq -r .refresh_token)

curl -fsS http://localhost:3000/api/profile \
  -H "Authorization: Bearer $ACCESS_TOKEN"
```

Rotate the refresh token and issue a new access token:

```bash
curl -fsS -X POST http://localhost:3000/refresh \
  -H 'Content-Type: application/json' \
  -d "{\"user_id\":\"generic-user\",\"refresh_token\":\"$REFRESH_TOKEN\"}"
```

Revoke the current access token and its associated refresh token:

```bash
curl -i -X POST http://localhost:3000/logout \
  -H "Authorization: Bearer $ACCESS_TOKEN"
```

After logout, the same access token receives `401 Unauthorized`. The example
uses one refresh token per user and demonstrates rotation, but its verify/write
sequence is intentionally simple. Production refresh endpoints should consume
and rotate tokens atomically, authenticate device/session identity, rate-limit
attempts, and detect replay.

## Signing And Encryption Key Rotation

Provide two signing keys of at least 32 bytes and two AES keys of exactly 32
bytes:

```bash
export AUTH_SIGNING_KEY_V1='signing-key-v1-that-is-at-least-32-bytes'
export AUTH_SIGNING_KEY_V2='signing-key-v2-that-is-at-least-32-bytes'
export AUTH_ENCRYPTION_KEY_V1='01234567890123456789012345678901'
export AUTH_ENCRYPTION_KEY_V2='12345678901234567890123456789012'

go run ./examples/auth -demo=rotation
```

The demo issues tokens using the v1 active keys, changes the active IDs to v2,
and verifies that v1 tokens remain valid while the old keys remain in the
keyring. Remove historical keys only after every token they signed or encrypted
has expired. Never reuse a key ID for different key material.

## Production Checklist

- Load keys from a secret manager or KMS; do not hardcode or log them.
- Configure `Issuer`, `Audience`, `MaxLifetime`, size limits, and revocation.
- Prefer context-aware generation and validation when key or revocation
  providers perform I/O.
- Use TLS and keep bearer/refresh tokens out of URLs and ordinary logs.
- Keep access tokens short-lived and make Redis revocation failures fail closed.
- Use `HttpOnly`, `Secure`, explicit `SameSite`, and narrow `Path` attributes
  for browser cookies.
- Add CSRF protection, abuse controls, atomic refresh rotation, and replay
  detection to real login/refresh/logout endpoints.

See the package-level [auth documentation](../../auth/README.md) for API and
migration details.

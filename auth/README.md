# Auth (v2)

Operational maturity, defaults and adoption gates are tracked in the [production matrix](../docs/PRODUCTION.md).

`auth` provides HS256 JWT signing, encrypted payloads, key rotation, revocation,
and a Redis session store. It has no HTTP framework dependency; middleware lives
under `auth/middleware/{nethttp,fiber,gin,echo}`.

## JWT policy

```go
manager, err := auth.NewManagerWithOptions(auth.ManagerOptions{
    SecretKey: os.Getenv("AUTH_JWT_SECRET"), // at least 32 bytes
    Issuer: "identity-service",
    Audience: []string{"orders-api"},
    MaxLifetime: 15 * time.Minute,
    Leeway: 30 * time.Second,
    Revocation: revocations, // optional auth.RevocationChecker
})
if err != nil { return err }
token, err := manager.GenerateTokenContext(ctx, auth.UserInfo{ID: "user-1"}, 15*time.Minute)
claims, err := manager.ValidateTokenContext(ctx, token)
```

All managers require `kid`, `exp`, `iat`, `nbf`, `sub`, and a nonempty `jti`.
Validation restricts the signing algorithm to HS256, enforces configured issuer,
all expected audiences, lifetime and size limits, and propagates revocation
backend errors. Revocation dependency failures preserve both
`errors.Is(err, auth.ErrAuthBackendUnavailable)` and the underlying cause.
All HTTP adapters return a generic 503 for this sentinel; invalid, expired,
and revoked credentials remain 401. Custom ContextTokenValidator implementations
can wrap the same sentinel to identify their dependency outages without exposing
the cause. Default limits are 16 KiB encoded token and 8 KiB payload.
Issuance rejects missing identities and nonpositive/over-policy lifetimes.

`GenerateToken`/`ValidateToken` are convenience methods for local providers;
use the context methods when keys or revocation require I/O. Legacy constructors,
legacy claim flags, and key lookup without `kid` have been removed in v2.

Keyrings are immutable copies:

```go
keys, err := auth.NewStaticKeyProvider("current", map[string][]byte{
    "previous": previousKey,
    "current": currentKey,
})
// Pass SigningKeys: keys instead of SecretKey.
```

Retain old key IDs until their tokens expire. Never reuse a key ID for different
key material. Implement `KeyProvider` for a secret manager or KMS; methods must
be concurrency-safe and honor their context.

## Encrypted payloads

`NewEncryptedManagerWithOptions` applies the same policy, with `AESKey` (exactly
32 bytes), optional `EncryptionKeys`, and `AADPolicy`. Set `auth.RequireAAD` when
binding tokens to session data. `auth.AADContext.Bytes()` encodes an unambiguous
binding; supply identical AAD during issuance and validation.

The encrypted format is version 1, carries `kid` and signed `ekid`, and combines
HS256 with AES-256-GCM. It is application-specific and is not JWE. TLS, secure
storage, CSRF controls, and authorization remain application responsibilities.

Generic encrypted payloads use the same encryption and claim engine:

```go
manager, err := auth.NewGenericEncryptedManagerWithOptions(
    auth.GenericEncryptedManagerOptions[MyPayload]{
        EncryptedManagerOptions: auth.EncryptedManagerOptions{
            SigningKeys: signingKeys, EncryptionKeys: encryptionKeys,
            Issuer: "identity", Audience: []string{"api"},
            MaxLifetime: time.Hour, AADPolicy: auth.RequireAAD,
        },
        Subject: func(value MyPayload) (string, error) { return value.UserID, nil },
    },
)
token, err := manager.GenerateTokenContext(ctx, payload, time.Hour, aad)
validated, err := manager.ValidateTokenContext(ctx, token, aad)
// validated.Payload and validated.RegisteredClaims retain identity/audit data.
```

## Generic signed claims and revocation

`NewGenericManagerWithOptions` supports custom `jwt.Claims` with all required
registered claims populated by the application. `TokenID` selects the revocation
key; supplying it requires a `Revocation` checker or Redis. Redis-backed JTI
revocation also requires `Namespace`; keys contain a SHA-256 digest of the ID.
Different projects/issuers must use different namespaces.

`NewGenericTokenValidator(manager, freshClaimsFactory, nil)` adapts the manager
to `GenericMiddleware`. The factory must allocate fresh claims per request.
Middleware validates with the request context and stores validated values via
`ctxkey.SetAuthClaims` and `ctxkey.SetUserInfo`. Errors returned to callers are
generic; authorization must use checked assertions and validated identity.

## Atomic refresh sessions

```go
sessions, err := auth.NewRedisSessionStore(auth.SessionStoreOptions{
    Redis: redisClient, // redis.UniversalClient, including Cluster/Sentinel
    Namespace: "orders:identity",
})
session, err := sessions.Create(ctx, userID, 7*24*time.Hour)
replacement, err := sessions.Rotate(ctx, session.ID, session.RefreshToken)
err = sessions.Revoke(ctx, session.ID)
```

Each sign-in creates an independent session ID and a 256-bit random refresh
credential. Redis stores only credential digests. Rotation consumes the old
credential and writes the replacement atomically within one Redis Cluster slot.
A replay of an already consumed credential revokes the entire session; a random
invalid credential does not revoke it. Unknown, expired, and revoked sessions
fail closed. Backend errors are returned rather than treated as valid sessions.

Session expiry is assigned once from Redis TIME and PEXPIREAT, remains identical
across rotations, and does not depend on application clock skew or reply latency. TTLs must be whole
milliseconds. Replay history is bounded by `MaxRotations` (default 1024, maximum
10000); exhausting it requires a new sign-in. A lost response after a committed
rotation requires reauthentication. Serialize refresh requests per session;
concurrent use of one credential is replay and invalidates the session.

To bind access tokens to session revocation, include `sid` in generic claims,
set `Revocation: sessions`, and make `TokenID` return that `sid`. Keep a distinct
random `jti` on every access token. Expired/logout/replayed sessions then reject
existing access tokens immediately. The runnable generic example demonstrates
this pattern, fail-closed login/refresh rate limits, and multiple sign-ins.

The old per-user verify-then-write refresh API is removed. Never log a refresh
session/token or put credentials in query strings. Production identity endpoints
must verify credentials and load roles from an authoritative store; the example
uses an explicitly fixed demonstration identity.

## Verification

```sh
go test -race ./auth/... ./examples/auth
```

Tests cover strict claims, HS256 restriction, signing/encryption rotation, AAD,
size limits, failed revocation lookups, session replay/concurrency/expiry/budgets,
and middleware context propagation. See [examples](../examples/auth/README.md).

Real Redis contracts are available with `-tags=integration` and
`EVENTBUS_REDIS_ADDR` / `EVENTBUS_REDIS_CLUSTER_ADDRS`. Supplying a fixture makes
connection or contract failures fatal; absent modes skip. These tests validate
TIME/PEXPIREAT expiry and atomic replay behavior on standalone and Cluster.

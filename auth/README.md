# Auth

The `auth` module provides standard HS256 JWT managers, encrypted JWT payloads
using AES-256 GCM, generic claims, Redis-backed refresh tokens, and token
revocation. Framework middleware is isolated by import path:

- `auth/middleware/nethttp`
- `auth/middleware/fiber`
- `auth/middleware/gin`
- `auth/middleware/echo`

All middleware validates a Bearer token and places `*auth.UserInfo` and the
validated registered claims in standard `context.Context` through
`ctxkey.SetUserInfo` and `ctxkey.SetAuthClaims`.

## Standard JWT

Load the signing key from a secret source and configure an explicit claim
policy. HMAC keys must contain at least 32 bytes.

```go
manager, err := auth.NewManagerWithOptions(auth.ManagerOptions{
    SecretKey:      os.Getenv("AUTH_JWT_SECRET"),
    Issuer:         "identity-service",
    Audience:       []string{"payments-api"},
    Leeway:         30 * time.Second,
    MaxLifetime:    time.Hour,
    MaxTokenSize:   16 * 1024,
    MaxPayloadSize: 8 * 1024,
    Revocation:     redisBackedManager,
})
if err != nil {
    return err
}

token, err := manager.GenerateToken(auth.UserInfo{
    ID:   "user-1",
    Role: "admin",
}, time.Hour)
```

The legacy constructors accept pre-upgrade tokens that have no `sub`, `jti`,
or `kid`. Options-based constructors are strict by default. Set
`AllowLegacyClaims` only for a bounded migration window; remove old keys and
disable the flag after the longest old-token lifetime has elapsed.

Issued tokens contain `exp`, `iat`, `nbf`, `sub`, and a random `jti`. User ID
and positive lifetime are required. Validation is restricted to HS256 and, when
configured, enforces issuer, every expected audience, expiration, size limits,
and revocation.

Standard `net/http` integration:

```go
protected := nethttpauth.Middleware(manager)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    value, ok := ctxkey.GetUserInfo(r.Context())
    user, valid := value.(*auth.UserInfo)
    if !ok || !valid || user == nil {
        http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
        return
    }
    // Use user.ID and user.Role.
}))
```

Fiber, Gin, and Echo expose equivalent `Middleware(manager)` functions.

## Key Rotation

Use a `KeyProvider` to issue with one active key while retaining previous keys
for verification. New tokens carry the active key ID in the `kid` header.

```go
keyring, err := auth.NewStaticKeyProvider("v2", map[string][]byte{
    "v1": oldSigningKey,
    "v2": newSigningKey,
})
manager, err := auth.NewManagerWithOptions(auth.ManagerOptions{
    SigningKeys: keyring,
    Issuer:      "identity-service",
    Audience:    []string{"payments-api"},
})
```

During migration from tokens that predate `kid`, use
`NewStaticKeyProviderWithLegacy(activeID, legacyID, keys)`. Key IDs must never
be reused for different key material. Implement `KeyProvider` to integrate a
KMS or secret manager; its methods receive the request context.

## Encrypted JWT

Encrypted managers hide claims from token holders and can bind authentication to
application-supplied Additional Authenticated Data (AAD):

```go
manager, err := auth.NewEncryptedManagerWithOptions(auth.EncryptedManagerOptions{
    SecretKey: os.Getenv("AUTH_JWT_SECRET"),
    AESKey:    os.Getenv("AUTH_AES_KEY"), // exactly 32 bytes
    AADPolicy: auth.RequireAAD,
})
if err != nil {
    return err
}

aad, err := (auth.AADContext{
    Issuer: "identity-service", Audience: "payments-api", SessionID: sessionID,
}).Bytes()
token, err := manager.GenerateToken(user, time.Hour, aad)
```

The same AAD must be supplied during validation. Generate binding values with a
cryptographically secure random source. Browser cookies carrying binding data
should use `HttpOnly`, `Secure`, an explicit `SameSite` policy, and a narrow
`Path`.

AAD and encrypted claims are defense in depth. They do not replace TLS, CSP and
XSS prevention, CSRF controls, secure token storage, rotation, or revocation.

This encrypted-token format is application-specific, versioned, and is not a
standards-compliant JWE. It signs public registered claims and an AES-256-GCM
ciphertext payload. For interoperability use a dedicated JWE implementation;
for centrally validated sessions consider a smaller opaque random token.

Encrypted key rotation uses separate `SigningKeys` and `EncryptionKeys` in
`EncryptedManagerOptions`. Tokens carry signing `kid` and encryption `ekid`.

## Generic Claims And Redis Sessions

`GenericManager[T]` and `GenericEncryptedManager[T]` support custom claims,
refresh tokens, and Redis-backed revocation:

```go
manager, err := auth.NewGenericManagerWithOptions(auth.GenericManagerOptions[*CustomClaims]{
    SecretKey:   os.Getenv("AUTH_JWT_SECRET"),
    Redis:       redisClient,
    Issuer:      "identity-service",
    Audience:    []string{"payments-api"},
    MaxLifetime: 15 * time.Minute,
    TokenID: func(claims *CustomClaims) (string, error) {
        return claims.ID, nil
    },
})
if err != nil {
    return err
}

refreshToken, err := manager.GenerateRefreshToken(ctx, userID, 7*24*time.Hour)
revoked, err := manager.IsRevoked(ctx, claims.ID)
err = manager.RevokeToken(ctx, claims.ID, time.Until(claims.ExpiresAt.Time))
```

To authenticate generic claims in net/http, Fiber, Gin, or Echo, build one
validator and pass it to that framework's `GenericMiddleware`. The claims
factory must return a new value for every request. Supplying a token-ID
extractor enables fail-closed Redis revocation checks:

```go
validator, err := auth.NewGenericTokenValidator(
    manager,
    func() *CustomClaims { return &CustomClaims{} },
	nil, // Manager options already enable fail-closed revocation.
)
if err != nil {
    return err
}

protected := nethttpauth.GenericMiddleware(validator)(handler)
```

Validated generic claims are available through both `ctxkey.GetUserInfo` and
`ctxkey.GetAuthClaims`. Omit the token-ID extractor only if access-token
revocation is intentionally disabled. Newly issued refresh tokens are stored as
SHA-256 digests in Redis; verification remains compatible with existing
plaintext entries during migration.

Use the request context for Redis operations. Do not silently treat a Redis
lookup failure as "not revoked" on security-sensitive endpoints.

## Deprecated Compatibility API

The following v1 APIs remain source-compatible but should not be used in new
code:

- `NewManager`: use `NewManagerWithOptions`.
- `NewEncryptedManager`: use `NewEncryptedManagerWithOptions`.
- `ExtractUserInfo`: use `ValidateTokenContext` so registered claims remain
  available.
- `GenericOptions` and `NewGenericManager`: use `GenericManagerOptions[T]` and
  `NewGenericManagerWithOptions`.

Compatibility constructors accept legacy token shapes and use static keys.
They are retained for the v1 line and are candidates for removal in v2.

## Security Rules

- Never hardcode or log signing keys, encryption keys, access tokens, or refresh
  tokens.
- Send bearer tokens in the `Authorization` header, not query strings.
- Return generic authentication errors and log internal causes server-side.
- Authenticate logout and derive the token ID from validated claims rather than
  trusting a caller-supplied JTI.
- Use checked context assertions.
- Protect refresh and revocation endpoints with explicit abuse controls.

See [`examples/auth`](../examples/auth) for runnable standard, encrypted,
generic Redis-backed, revocation, refresh-rotation, and key-rotation flows.

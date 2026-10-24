# Auth

The `auth` module provides standard HS256 JWT managers, encrypted JWT payloads
using AES-256 GCM, generic claims, Redis-backed refresh tokens, and token
revocation. Framework middleware is isolated by import path:

- `auth/middleware/nethttp`
- `auth/middleware/fiber`
- `auth/middleware/gin`
- `auth/middleware/echo`

All middleware validates a Bearer token and places `*auth.UserInfo` in standard
`context.Context` through `ctxkey.SetUserInfo`.

## Standard JWT

Load the signing key from a secret source. It must contain at least 32
characters.

```go
secret := os.Getenv("AUTH_JWT_SECRET")
manager, err := auth.NewManager(secret)
if err != nil {
    return err
}

token, err := manager.GenerateToken(auth.UserInfo{
    ID:   "user-1",
    Role: "admin",
}, time.Hour)
```

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

## Encrypted JWT

Encrypted managers hide claims from token holders and can bind authentication to
application-supplied Additional Authenticated Data (AAD):

```go
manager, err := auth.NewEncryptedManager(
    os.Getenv("AUTH_JWT_SECRET"),
    os.Getenv("AUTH_AES_KEY"), // exactly 32 bytes
)
if err != nil {
    return err
}

token, err := manager.GenerateToken(user, time.Hour, []byte(sessionID))
```

The same AAD must be supplied during validation. Generate binding values with a
cryptographically secure random source. Browser cookies carrying binding data
should use `HttpOnly`, `Secure`, an explicit `SameSite` policy, and a narrow
`Path`.

AAD and encrypted claims are defense in depth. They do not replace TLS, CSP and
XSS prevention, CSRF controls, secure token storage, rotation, or revocation.

## Generic Claims And Redis Sessions

`GenericManager[T]` and `GenericEncryptedManager[T]` support custom claims,
refresh tokens, and Redis-backed revocation:

```go
manager, err := auth.NewGenericManager[*CustomClaims](auth.GenericOptions{
    SecretKey: os.Getenv("AUTH_JWT_SECRET"),
    Redis:     redisClient,
})
if err != nil {
    return err
}

refreshToken, err := manager.GenerateRefreshToken(ctx, userID, 7*24*time.Hour)
revoked, err := manager.IsRevoked(ctx, claims.ID)
err = manager.RevokeToken(ctx, claims.ID, time.Until(claims.ExpiresAt.Time))
```

Use the request context for Redis operations. Do not silently treat a Redis
lookup failure as "not revoked" on security-sensitive endpoints.

## Security Rules

- Never hardcode or log signing keys, encryption keys, access tokens, or refresh
  tokens.
- Send bearer tokens in the `Authorization` header, not query strings.
- Return generic authentication errors and log internal causes server-side.
- Authenticate logout and derive the token ID from validated claims rather than
  trusting a caller-supplied JTI.
- Use checked context assertions.
- Protect refresh and revocation endpoints with explicit abuse controls.

See [`examples/auth`](../examples/auth) for one comparable standard-JWT flow on
all four HTTP stacks.

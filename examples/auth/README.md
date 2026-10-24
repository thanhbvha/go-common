# Auth Example

The runnable example compares the same standard-JWT login and protected-profile
flow across standard `net/http`, Fiber, Gin, and Echo.

## Run

Set a signing secret of at least 32 characters:

```powershell
$env:AUTH_JWT_SECRET="replace-with-a-random-secret-at-least-32-characters"
go run ./examples/auth -stack=nethttp
```

Valid stacks are `nethttp`, `fiber`, `gin`, and `echo`. Every stack listens on
`http://localhost:3000`.

Issue a token with `POST /login`, then send it only through the Authorization
header:

```powershell
$login = Invoke-RestMethod -Method Post http://localhost:3000/login
Invoke-RestMethod http://localhost:3000/api/profile -Headers @{ Authorization = "Bearer $($login.access_token)" }
```

The example never logs tokens, accepts them in URLs, or hardcodes signing keys.
Encrypted and generic Redis-backed managers are documented in
[`auth/README.md`](../../auth/README.md); they are intentionally not duplicated
for every framework here.

## Security

- Load signing and encryption keys from a secret manager or environment.
- Use TLS in deployment and keep bearer tokens out of URLs and ordinary logs.
- AAD binds encrypted ciphertext to application-supplied context. It is defense
  in depth, not a replacement for XSS prevention, CSRF controls, secure cookies,
  token rotation, and revocation.
- Read authenticated users from `context.Context` through `ctxkey.GetUserInfo`.

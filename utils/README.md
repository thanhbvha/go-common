# Utils

Production-oriented helpers that complement the Go standard library. The module
contains focused packages for cryptography, context values, graceful shutdown,
slice transformations, and strings.

## Cryptography

`utils/crypt` exposes authenticated encryption only. Ciphertext contains a
random nonce prefix and must be stored together with the AAD needed to decrypt
it.

```go
key, err := crypt.GenerateKey32()
if err != nil {
	return err
}

aad := []byte("tenant:example")
ciphertext, err := crypt.EncryptAESGCM(key, []byte("secret"), aad)
if err != nil {
	return err
}
plaintext, err := crypt.DecryptAESGCM(key, ciphertext, aad)
if err != nil {
	return err
}
```

`EncryptChaCha20` and `DecryptChaCha20` provide the same contract with
ChaCha20-Poly1305. Decryption authentication failures match
`crypt.ErrAuthenticationFailed` through `errors.Is`.

Passwords use Argon2id with bounded parsing and OWASP's 19 MiB, two-iteration,
single-lane baseline:

```go
encoded, err := crypt.HashPassword(password)
if err != nil {
	return err
}
match, err := crypt.VerifyPassword(password, encoded)
if err != nil {
	return err // malformed or unsupported stored hash
}
```

After a successful login, call `PasswordHashNeedsUpgrade` and re-hash when it
returns true. Fast digest helpers are available for checksums and signatures:
`HashSHA256`, `HashSHA512`, `HashSHA3_256`, and `HashBLAKE2b`. They are not
password hash functions.

AES-CBC, MD5, and SHA-1 helpers were removed. Existing legacy data should be
migrated at its owning boundary instead of reintroducing unauthenticated or
collision-prone primitives here.

## Context values

`utils/ctxkey` keeps `context.Context` keys private and exposes accessors for
user ID, client IP, request ID, user information, auth claims, and DataLoader.

```go
ctx = ctxkey.SetRequestID(ctx, "req-123")
requestID, ok := ctxkey.GetRequestID(ctx)
```

Gin, Echo, and Fiber local stores require string keys. Use
`ctxkey.LocalUserInfo` and `ctxkey.LocalAuthClaims` for those stores; these are
separate from the private context keys.

## Graceful shutdown

`utils/graceful` runs hooks once in reverse registration order. All hooks share
one timeout. Errors and recovered hook panics are joined and returned to the
caller.

```go
shutdown := graceful.NewShutdown(15 * time.Second)
shutdown.Register(func(ctx context.Context) error {
	return server.Shutdown(ctx)
})
shutdown.Register(func(ctx context.Context) error {
	return database.Close()
})

if err := shutdown.Wait(); err != nil {
	return fmt.Errorf("shutdown: %w", err)
}
```

`WaitContext` also begins shutdown when its parent context is canceled.
`Execute` starts shutdown programmatically. Repeated or concurrent calls return
the result of the first execution. Hooks must honor context cancellation;
shutdown cannot forcibly stop a blocking function.

## Slices and strings

`utils/slice` provides stable `Unique`, `Filter`, and `Map`. It preserves nil
inputs. Use the standard library `slices` package for `Contains`, sorting, and
other standard operations, and the standard library `maps` package for maps.

```go
unique := slice.Unique([]int{3, 1, 3, 2})
even := slice.Filter(unique, func(value int) bool { return value%2 == 0 })
labels := slice.Map(even, strconv.Itoa)
```

`utils/str.Random` returns a cryptographically secure alphanumeric string and
propagates entropy-source failures:

```go
referenceID, err := str.Random(16)
if err != nil {
	return err
}
slug := str.Slugify("Xin chào Việt Nam")
```

Negative random lengths return an error. `Slugify` transliterates Unicode text
using `github.com/gosimple/slug`.

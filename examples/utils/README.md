# Utils example

The example in `main.go` demonstrates every public utils package with explicit
error handling:

- AES-256-GCM and ChaCha20-Poly1305 authenticated encryption.
- Argon2id password hashing and verification.
- SHA-2, SHA-3, and BLAKE2b checksum helpers.
- Secure random strings and Unicode slug generation.
- Collision-resistant context accessors.
- Stable slice deduplication, filtering, and mapping. Standard operations such
  as `Contains` use Go's `slices` package.
- One-shot graceful shutdown with LIFO hooks and a shared timeout.

Run it and press Ctrl+C after the examples finish:

```bash
go run ./examples/utils
```

The example deliberately does not print encryption keys or password hashes.
All encryption examples use authenticated encryption; AES-CBC is no longer part
of the module.

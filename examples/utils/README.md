# Utils Module

## Overview
The `utils` directory is a collection of helper packages designed to standardize common operations across microservices, such as cryptography, string manipulation, context keys, generic collections, and graceful shutdown coordination.

## Sub-Packages and Covered Examples
This example module contains a central `main.go` file demonstrating all sub-packages:

1. **`RunCryptExample()`**: Modern, secure cryptographic utilities.
   - Implements `AES-256 GCM` (AEAD, Recommended), `ChaCha20-Poly1305` (Fast AEAD for mobile/IoT), and `AES-256 CBC` (Legacy).
   - Implements `Argon2id` for OWASP-recommended password hashing.
   - Provides fast data hashing (`SHA-256`, `SHA3-256`, `BLAKE2b`).
   - Provides cryptographically secure random number generators (`GenerateKey32`).
2. **`RunStrExample()`**: Helpers for string manipulation (e.g., `Slugify`, `Random`).
3. **`RunCtxKeyExample()`**: Strongly-typed context keys to prevent context value collisions (e.g., `ctxkey.UserIDKey`, `ctxkey.RequestIDKey`).
4. **`RunSliceExample()`**: Go 1.18+ Generics for Slices. Includes `slice.Contains`, `slice.Unique`, `slice.Filter`, and `slice.Map` so you never have to write manual `for-range` loops again!
5. **`RunMapsExample()`**: Go 1.18+ Generics for Maps. Quickly extract `maps.Keys` or `maps.Values`.
6. **`RunGracefulExample()`**: A centralized coordinator that traps OS signals (SIGINT/SIGTERM) and triggers registered shutdown hooks in reverse order (LIFO) with a timeout.

## 🚨 Best Practices for AI/Developers
- **Cryptography (CRITICAL)**: Always use `AES-256 GCM` (`EncryptAESGCM`) for new symmetric encryption needs. Do NOT use CBC unless maintaining legacy systems, as it is vulnerable to padding oracle attacks. For passwords, ALWAYS use `HashPasswordArgon2id`; never use MD5, SHA1, or plain bcrypt.
- **Context Keys**: Never use raw strings as keys when calling `context.WithValue`. Always use the predefined types in the `ctxkey` package.
- **Generics vs Manual Loops**: Always check if `utils/slice` or `utils/maps` has the utility you need before manually writing a `for` loop to check for element existence or uniqueness.

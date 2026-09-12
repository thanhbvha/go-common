# Utils Module

## Overview
The `utils` directory is a collection of helper packages designed to standardize common operations across microservices, such as cryptography, string manipulation, context keys, and graceful shutdown coordination.

## Sub-Packages
- **`crypt`**: Modern, secure cryptographic utilities.
  - Implements `AES-256 GCM` (AEAD, Recommended), `ChaCha20-Poly1305` (Fast AEAD for mobile/IoT), and `AES-256 CBC` (Legacy).
  - Implements `Argon2id` for OWASP-recommended password hashing.
  - Provides cryptographically secure random number generators (`GenerateKey32`, `GenerateRandomString`).
- **`graceful`**: A centralized coordinator that traps OS signals (SIGINT/SIGTERM) and triggers registered shutdown hooks in reverse order.
- **`ctxkey`**: Strongly-typed context keys to prevent context value collisions (e.g., `ctxkey.UserInfo`, `ctxkey.RequestID`).
- **`str`**: Helpers for string manipulation (e.g., `Slugify`, `Random`).

## 🚨 Best Practices for AI/Developers
- **Cryptography (CRITICAL)**: Always use `AES-256 GCM` (`EncryptAESGCM`) for new symmetric encryption needs. Do NOT use CBC unless maintaining legacy systems, as it is vulnerable to padding oracle attacks. For passwords, ALWAYS use `HashPasswordArgon2id`; never use MD5, SHA1, or plain bcrypt.
- **Context Keys**: Never use raw strings as keys when calling `context.WithValue`. Always use the predefined types in the `ctxkey` package.

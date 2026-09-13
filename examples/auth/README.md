# Auth Module

## Overview
The `auth` module provides an ultra-secure JWT management system with built-in framework integrations. It offers two distinct strategies depending on your security requirements:
1. **Standard JWT (`auth.Manager`)**: Uses standard Base64-encoded JWTs. Best for generic, non-sensitive payloads.
2. **Encrypted JWT (`auth.EncryptedManager`)**: Implements JWE-like AES-256 GCM encryption. Guarantees that sensitive claims (like user roles, internal IDs, or metadata) cannot be inspected or tampered with by clients.

## Key Features
- `auth.NewManager(jwtSecret)`: Initializes the Standard JWT manager.
- `auth.NewEncryptedManager(jwtSecret, aesKey)`: Initializes the Encrypted JWT manager.
- **AAD (Additional Authenticated Data)**: Cryptographically binds the encrypted token to a specific context (like a Session ID or Device ID) to prevent token theft/replay attacks (XSS/CSRF mitigation).
- **Framework Agnostic Middlewares**: Ready-to-use middlewares that parse, validate/decrypt, and inject `UserInfo` into the request context for **Fiber**, **Gin**, and **Echo**.

## 🚨 Best Practices for AI/Developers
- **Choose the Right Strategy**: Default to `EncryptedManager` unless you explicitly need clients (like frontend SPAs) to parse and read the token payload natively without an API call.
- **Secret Keys (CRITICAL)**: **NEVER** hardcode the `jwtSecret` or `aesKey` in source code. They must be loaded from secure environment variables. The `aesKey` MUST be exactly 32 bytes for AES-256 GCM.
- **AAD Binding (Encrypted JWT)**: Always leverage AAD when generating encrypted tokens for browsers. E.g., generate a secure random `SessionID`, set it in an `HttpOnly` Cookie, and pass it as the `aad` byte array to `GenerateToken`. In your middleware's `aadExtractor`, read this cookie.
- **Context Extraction**: Inside your route handlers, extract the user using the framework's native context bag. Do NOT parse the token manually in handlers.
  - Fiber: `c.Locals(string(ctxkey.UserInfo)).(*auth.UserInfo)`
  - Gin: `c.Get(string(ctxkey.UserInfo))`
  - Echo: `c.Get(string(ctxkey.UserInfo)).(*auth.UserInfo)`

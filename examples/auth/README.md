# Auth Module

## Overview
The `auth` module provides an ultra-secure JWT management system. Unlike standard JWTs where the payload is public (Base64 encoded), this module implements **Encrypted JWTs (JWE-like)** using AES-256 GCM. This guarantees that sensitive claims (like user roles or metadata) cannot be inspected or tampered with by clients.

## Key Features
- `auth.NewEncryptedManager(jwtSecret, aesKey)`: Creates the token manager.
- `manager.GenerateToken(userInfo, duration, aad)`: Generates an encrypted JWT.
- **AAD (Additional Authenticated Data)**: Allows cryptographically binding the token to a specific context (like a Session ID or Device ID) to prevent token theft/replay attacks.
- `auth.FiberEncryptedMiddleware`: A ready-to-use middleware for the Fiber framework that parses, decrypts, and injects `UserInfo` into the request context.

## 🚨 Best Practices for AI/Developers
- **Secret Keys (CRITICAL)**: **NEVER** hardcode the `jwtSecret` or `aesKey` in source code. They must be loaded from secure environment variables. The `aesKey` MUST be exactly 32 bytes.
- **AAD Binding**: Always leverage AAD when generating tokens. E.g., generate a secure random `SessionID`, set it in an `HttpOnly` Cookie, and pass it as the `aad` byte array to `GenerateToken`. In your middleware's `aadExtractor`, read this cookie. This effectively mitigates XSS token-stealing attacks.
- **Context Extraction**: Inside your route handlers, extract the user using `c.Locals(string(ctxkey.UserInfo)).(*auth.UserInfo)`. Do NOT parse the token manually in handlers.

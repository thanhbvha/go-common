# Auth Module

## Overview
The `auth` module provides an ultra-secure JWT management system with built-in framework integrations. It offers three distinct strategies depending on your security requirements:
1. **Standard JWT (`auth.Manager`)**: Uses standard Base64-encoded JWTs. Best for simple, non-sensitive payloads.
2. **Encrypted JWT (`auth.EncryptedManager`)**: Implements JWE-like AES-256 GCM encryption. Guarantees that sensitive claims (like user roles, internal IDs, or metadata) cannot be inspected or tampered with by clients.
3. **Generic JWT & Session Control (`auth.GenericManager[T]`)**: Extends standard JWTs to allow custom claims (any Go struct) and provides full session control via Redis (Refresh Tokens and Token Revocation/Blacklist).
4. **Generic Encrypted JWT & Session Control (`auth.GenericEncryptedManager[T]`)**: The ultimate security combo. Custom claims encrypted with AES-256 GCM, plus full Redis session control.

## Key Features
- `auth.NewManager(jwtSecret)`: Initializes the Standard JWT manager.
- `auth.NewEncryptedManager(jwtSecret, aesKey)`: Initializes the Encrypted JWT manager.
- `auth.NewGenericManager[T](options)`: Initializes the Generic manager with custom claims and Redis backing.
- `auth.NewGenericEncryptedManager[T](options)`: Initializes the Generic Encrypted manager.
- **Session Control (Redis)**: `GenericManager` and `GenericEncryptedManager` support generating refresh tokens (`GenerateRefreshToken`), blacklisting compromised tokens (`RevokeToken`), and checking validity (`VerifyRefreshToken`, `IsRevoked`).
- **AAD (Additional Authenticated Data)**: Cryptographically binds the encrypted token to a specific context (like a Session ID or Device ID) to prevent token theft/replay attacks (XSS/CSRF mitigation).
- **Framework Agnostic Middlewares**: Ready-to-use middlewares that parse, validate/decrypt, and inject `UserInfo` into the request context for **Fiber**, **Gin**, and **Echo**.

## Covered Examples
This directory includes a complete `main.go` file with the following runnable scenarios:
1. `RunEncryptedJWT_Fiber()`: Showcases AAD binding and payload encryption using Fiber.
2. `RunStandardJWT_Gin()`: Basic standard JWT usage with Gin framework.
3. `RunStandardJWT_Echo()`: Basic standard JWT usage with Echo framework.
4. `RunGenericJWT_Fiber()`: Demonstrates how to use custom Struct Claims, issue Refresh Tokens, and revoke access tokens using a Redis blacklist.
5. `RunGenericEncryptedJWT_Example()`: Demonstrates encrypting a custom Struct Claim (AES-256 GCM) with Redis session capabilities.

## 🚨 Best Practices for AI/Developers
- **Choose the Right Strategy**: 
  - Default to `EncryptedManager` or `GenericEncryptedManager[T]` to hide sensitive data and prevent token theft.
  - Use generic variants when you need to maintain sessions, log users out globally, or use custom payloads.
- **Secret Keys (CRITICAL)**: **NEVER** hardcode the `jwtSecret` or `aesKey` in source code. They must be loaded from secure environment variables. The `aesKey` MUST be exactly 32 bytes for AES-256 GCM.
- **AAD Binding (Encrypted JWT)**: Always leverage AAD when generating encrypted tokens for browsers. E.g., generate a secure random `SessionID`, set it in an `HttpOnly` Cookie, and pass it as the `aad` byte array to `GenerateToken`. In your middleware's `aadExtractor`, read this cookie.
- **Token Revocation (Generic JWT)**: When a user logs out, always call `manager.RevokeToken()` using the `jti` (JWT ID) of the access token, and verify it using `manager.IsRevoked()` in your middlewares to prevent reuse of stolen tokens.
- **Context Extraction**: Inside your route handlers, extract the user using the framework's native context bag. Do NOT parse the token manually in handlers.
  - Fiber: `c.Locals(string(ctxkey.UserInfo)).(*auth.UserInfo)`
  - Gin: `c.Get(string(ctxkey.UserInfo))`
  - Echo: `c.Get(string(ctxkey.UserInfo)).(*auth.UserInfo)`

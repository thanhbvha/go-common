# Auth Module

The `auth` module provides an enterprise-grade authentication and authorization toolkit for Go microservices. It supports **Standard JWTs** as well as an advanced **Encrypted Payload** mechanism (JWE-like) using AES-256 GCM.

This module is designed to prevent Token Theft by implementing **Dynamic AAD (Additional Authenticated Data) / Token Binding**, allowing you to bind a token cryptographically to a specific Browser Session, Device ID, or Client IP.

---

## 🌟 Key Features

1. **Standard JWT (`auth.Manager`)**: Standard HS256 signing via `golang-jwt`.
2. **Encrypted JWT (`auth.EncryptedManager`)**: Encrypts the entire JWT payload using `AES-256 GCM` before signing it. The payload is completely hidden from attackers and cannot be decoded on sites like `jwt.io`.
3. **Dynamic AAD / Token Binding**: Protect against XSS and Token Theft by binding the encrypted token to a `Session ID` (stored in an `HttpOnly` Cookie) or a `Device ID` (Custom Header).
4. **High Performance**: Uses `github.com/goccy/go-json` for ultra-fast JSON serialization/deserialization.
5. **Framework-Agnostic Middlewares**:
   - `auth.FiberMiddleware` / `auth.FiberEncryptedMiddleware`
   - `auth.GinMiddleware` / `auth.GinEncryptedMiddleware`
   - `auth.EchoMiddleware` / `auth.EchoEncryptedMiddleware`
6. **Context Injection**: Automatically extracts the token, verifies/decrypts it, and injects the `UserInfo` struct into the framework's context.

---

## 📦 Installation

```bash
go get github.com/thanhbvha/go-common/auth
```

---

## 🚀 Usage Guide

### 1. Standard JWT (Unencrypted Payload)
Use this if you just need standard JWTs.

```go
manager := auth.NewManager("my-jwt-secret-key")

// Generate
user := auth.UserInfo{ID: "user_1", Role: "admin"}
token, err := manager.GenerateToken(user, 24*time.Hour)

// Validate
claims, err := manager.ValidateToken(token)
```

### 2. Encrypted JWT (No AAD)
Use this to hide sensitive data inside the payload. The payload becomes an unreadable Base64 string.

```go
// AES Key MUST be exactly 32 bytes for AES-256 GCM
manager, _ := auth.NewEncryptedManager("jwt-secret", "12345678901234567890123456789012")

// Generate (Pass nil for AAD)
user := auth.UserInfo{ID: "user_1", Role: "admin"}
token, err := manager.GenerateToken(user, 24*time.Hour, nil)

// Validate (Pass nil for AAD)
userInfo, err := manager.ValidateToken(token, nil)
```

### 3. Extreme Security: Encrypted JWT + Dynamic AAD (Token Binding)
Use this pattern for highly sensitive applications (e.g., Finance, Wallets). 
It binds the JWT to a specific browser session using a randomly generated `Session ID` stored in a secure `HttpOnly` Cookie.

If a hacker steals the JWT from LocalStorage via XSS, they cannot decrypt it because they cannot steal the `HttpOnly` Cookie!

```go
package main

import (
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/auth"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

func main() {
	manager, _ := auth.NewEncryptedManager("jwt-secret", "12345678901234567890123456789012")
	app := fiber.New()

	// --- 1. LOGIN API ---
	app.Post("/login", func(c *fiber.Ctx) error {
		user := auth.UserInfo{ID: "user_777", Role: "admin"}

		// Generate a random Session ID (e.g., using UUID)
		sessionID := "sess_" + fmt.Sprintf("%d", time.Now().UnixNano())

		// Set Session ID in an HttpOnly cookie (Immune to XSS)
		c.Cookie(&fiber.Cookie{
			Name:     "session_id",
			Value:    sessionID,
			Expires:  time.Now().Add(24 * time.Hour),
			HTTPOnly: true,
		})

		// Encrypt and bind the token to the Session ID (AAD)
		token, _ := manager.GenerateToken(user, 24*time.Hour, []byte(sessionID))

		// Return the token (Client saves this in LocalStorage or Memory)
		return c.JSON(fiber.Map{"access_token": token})
	})

	// --- 2. PROTECTED ROUTE ---
	api := app.Group("/api")

	// Middleware Extractor: Tells the middleware how to find the AAD for decryption
	aadExtractor := func(c *fiber.Ctx) []byte {
		sessionID := c.Cookies("session_id")
		if sessionID == "" {
			return nil
		}
		return []byte(sessionID)
	}

	// Apply Middleware
	api.Use(auth.FiberEncryptedMiddleware(manager, aadExtractor))

	// API Handler
	api.Get("/profile", func(c *fiber.Ctx) error {
		// Context Injection: Get UserInfo safely
		val := c.Locals(string(ctxkey.UserInfo))
		userInfo := val.(*auth.UserInfo)

		return c.JSON(fiber.Map{
			"message": "Protected Data",
			"user_id": userInfo.ID,
		})
	})

	app.Listen(":3000")
}
```

---

## 🛡️ Security Best Practices
1. **Never Hardcode Secrets**: Always load your `jwtSecret` and `aesKey` from environment variables (`.env`) or a Secret Manager.
2. **AES Key Size**: The `aesKey` passed to `NewEncryptedManager` **MUST** be exactly 32 bytes long.
3. **Use HTTPS**: `HttpOnly` cookies are only safe against MITM attacks if you enforce HTTPS in production (set `Secure: true` on your cookies).

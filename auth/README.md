# Auth Module

The `auth` module provides an enterprise-grade authentication and authorization toolkit. It goes beyond standard JWTs by providing an **Encrypted Payload** mechanism (JWE-like) using AES-256 GCM or ChaCha20-Poly1305.

This completely hides sensitive User Information (such as IDs, roles, or internal metadata) from attackers if a token is intercepted or leaked.

## Features

1. **Standard JWT (`auth.Manager`)**: Standard RS256/HS256 signing via `golang-jwt`.
2. **Encrypted JWT (`auth.EncryptedManager`)**: Encrypts the entire JWT payload using `AES-256 GCM` before signing it. The payload cannot be decoded at `jwt.io`.
3. **Framework-Agnostic Middlewares**:
   - `auth.FiberMiddleware` / `auth.FiberEncryptedMiddleware`
   - `auth.GinMiddleware` / `auth.GinEncryptedMiddleware`
   - `auth.EchoMiddleware` / `auth.EchoEncryptedMiddleware`
4. **Context Injection**: Automatically extracts the token, verifies/decrypts it, and injects the `UserInfo` struct into the request context (accessible via `ctxkey.UserInfo`).

## Installation

```bash
go get github.com/thanhbvha/go-common/auth
```

## Usage: Encrypted JWT with Fiber

```go
package main

import (
	"time"
	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/auth"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

func main() {
	// AES-256 Key MUST be exactly 32 bytes!
	manager, _ := auth.NewEncryptedManager("jwt-secret", "12345678901234567890123456789012")

	app := fiber.New()

	app.Post("/login", func(c *fiber.Ctx) error {
		user := auth.UserInfo{ ID: "u_1", Role: "admin" }
		token, _ := manager.GenerateToken(user, 24*time.Hour)
		return c.JSON(fiber.Map{"token": token})
	})

	api := app.Group("/api", auth.FiberEncryptedMiddleware(manager))

	api.Get("/profile", func(c *fiber.Ctx) error {
		// Context Injection
		userInfo := c.Locals(string(ctxkey.UserInfo)).(*auth.UserInfo)
		return c.JSON(userInfo)
	})

	app.Listen(":3000")
}
```

## Security Note
Always store your `jwtSecret` and `aesKey` in environment variables or a secure secret manager. Never hardcode them in production!

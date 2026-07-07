package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/auth"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

func main() {
	fmt.Println("=== Auth Module Example ===")

	// 1. Initialize the Encrypted JWT Manager
	// The AES key must be EXACTLY 32 bytes for AES-256 GCM!
	jwtSecret := "super-secret-jwt-key"
	aesKey := "12345678901234567890123456789012"

	manager, err := auth.NewEncryptedManager(jwtSecret, aesKey)
	if err != nil {
		log.Fatalf("Failed to initialize auth manager: %v", err)
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	// 2. Public Login Endpoint
	app.Post("/login", func(c *fiber.Ctx) error {
		// Mock User Validation...
		user := auth.UserInfo{
			ID:    "user_777",
			Role:  "admin",
			Email: "admin@example.com",
			Metadata: map[string]interface{}{
				"plan": "premium",
			},
		}

		// Generate an Encrypted Token valid for 24 hours
		token, err := manager.GenerateToken(user, 24*time.Hour)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to generate token"})
		}

		// Look at the token string - you will see that the payload is completely
		// unreadable base64 (encrypted), not the standard base64 JSON payload of standard JWTs.
		fmt.Println("Generated Encrypted Token:", token)

		return c.JSON(fiber.Map{
			"access_token": token,
		})
	})

	// 3. Protected Route Group using Middleware
	api := app.Group("/api")
	
	// Apply the Fiber Encrypted Middleware
	api.Use(auth.FiberEncryptedMiddleware(manager))

	api.Get("/profile", func(c *fiber.Ctx) error {
		// The Middleware has automatically parsed, decrypted, and injected UserInfo into context!
		
		// Retrieve UserInfo from Context
		val := c.Locals(string(ctxkey.UserInfo))
		userInfo, ok := val.(*auth.UserInfo)
		if !ok {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to parse user info from context"})
		}

		return c.JSON(fiber.Map{
			"message": "Welcome to the protected area!",
			"user":    userInfo,
		})
	})

	// 4. Start Server
	fmt.Println("Server listening on http://localhost:3000")
	fmt.Println("1. Send a POST request to /login to get a token")
	fmt.Println("2. Use the token in the 'Authorization: Bearer <token>' header to access GET /api/profile")
	
	log.Fatal(app.Listen(":3000"))
}

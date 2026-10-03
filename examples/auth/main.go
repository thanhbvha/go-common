package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"

	"github.com/thanhbvha/go-common/auth"
	"github.com/thanhbvha/go-common/auth/middleware/echo"
	"github.com/thanhbvha/go-common/auth/middleware/fiber"
	"github.com/thanhbvha/go-common/auth/middleware/gin"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

func main() {
	fmt.Println("=== Auth Module Examples ===")
	fmt.Println("Uncomment one of the functions below to run a specific example.")

	// RunEncryptedJWT_Fiber()
	// RunStandardJWT_Gin()
	// RunStandardJWT_Echo()
	// RunGenericJWT_Fiber()
	RunGenericEncryptedJWT_Example()
}

// =====================================================================
// 1. Encrypted JWT with Fiber Framework (Highly Recommended for Security)
// =====================================================================
func RunEncryptedJWT_Fiber() {
	fmt.Println("--- Starting Encrypted JWT (Fiber) ---")

	// 1. Initialize the Encrypted JWT Manager
	// CRITICAL: The AES key MUST be EXACTLY 32 bytes to ensure AES-256 GCM encryption.
	// Never hardcode keys in production. Read them from environment variables or a Secret Manager.
	jwtSecret := "super-secret-jwt-key"
	aesKey := "12345678901234567890123456789012"

	manager, err := auth.NewEncryptedManager(jwtSecret, aesKey)
	if err != nil {
		log.Fatalf("Failed to initialize auth manager: %v", err)
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	// Login Endpoint
	app.Post("/login", func(c *fiber.Ctx) error {
		user := auth.UserInfo{ID: "user_777", Role: "admin"}
		sessionID := "sess_" + fmt.Sprintf("%d", time.Now().UnixNano())

		// AAD Binding: Set Session ID in Cookie
		c.Cookie(&fiber.Cookie{
			Name:     "session_id",
			Value:    sessionID,
			Expires:  time.Now().Add(24 * time.Hour),
			HTTPOnly: true,
		})

		token, err := manager.GenerateToken(user, 24*time.Hour, []byte(sessionID))
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to generate token"})
		}

		fmt.Println("Generated Encrypted Token:", token)
		return c.JSON(fiber.Map{"access_token": token})
	})

	// Protected Route
	api := app.Group("/api")
	aadExtractor := func(c *fiber.Ctx) []byte {
		sessionID := c.Cookies("session_id")
		if sessionID == "" {
			return nil
		}
		return []byte(sessionID)
	}

	api.Use(fiberauth.EncryptedMiddleware(manager, aadExtractor))
	api.Get("/profile", func(c *fiber.Ctx) error {
		userInfo := c.Locals(string(ctxkey.UserInfo)).(*auth.UserInfo)
		return c.JSON(fiber.Map{"message": "Secure Fiber Profile", "user": userInfo})
	})

	log.Fatal(app.Listen(":3000"))
}

// =====================================================================
// 2. Standard JWT with Gin Framework
// =====================================================================
func RunStandardJWT_Gin() {
	fmt.Println("--- Starting Standard JWT (Gin) ---")

	jwtSecret := "super-secret-jwt-key-which-is-at-least-32-bytes"
	manager, err := auth.NewManager(jwtSecret)
	if err != nil {
		log.Fatalf("Failed to initialize auth manager: %v", err)
	}

	r := gin.Default()

	// Login Endpoint
	r.POST("/login", func(c *gin.Context) {
		user := auth.UserInfo{ID: "user_888", Role: "user"}
		token, err := manager.GenerateToken(user, 24*time.Hour)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
			return
		}

		fmt.Println("Generated Standard Token:", token)
		c.JSON(http.StatusOK, gin.H{"access_token": token})
	})

	// Protected Route
	api := r.Group("/api")

	// Apply Standard Gin Middleware
	api.Use(ginauth.Middleware(manager))

	api.GET("/profile", func(c *gin.Context) {
		val, exists := c.Get(string(ctxkey.UserInfo))
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
			return
		}
		userInfo := val.(*auth.UserInfo)
		c.JSON(http.StatusOK, gin.H{"message": "Secure Gin Profile", "user": userInfo})
	})

	log.Fatal(r.Run(":3001"))
}

// =====================================================================
// 3. Standard JWT with Echo Framework
// =====================================================================
func RunStandardJWT_Echo() {
	fmt.Println("--- Starting Standard JWT (Echo) ---")

	jwtSecret := "super-secret-jwt-key-which-is-at-least-32-bytes"
	manager, err := auth.NewManager(jwtSecret)
	if err != nil {
		log.Fatalf("Failed to initialize auth manager: %v", err)
	}

	e := echo.New()

	// Login Endpoint
	e.POST("/login", func(c echo.Context) error {
		user := auth.UserInfo{ID: "user_999", Role: "editor"}
		token, err := manager.GenerateToken(user, 24*time.Hour)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to generate token"})
		}

		fmt.Println("Generated Standard Token:", token)
		return c.JSON(http.StatusOK, map[string]string{"access_token": token})
	})

	// Protected Route
	api := e.Group("/api")

	// Apply Standard Echo Middleware
	api.Use(echoauth.Middleware(manager))

	api.GET("/profile", func(c echo.Context) error {
		userInfo := c.Get(string(ctxkey.UserInfo)).(*auth.UserInfo)
		return c.JSON(http.StatusOK, map[string]interface{}{"message": "Secure Echo Profile", "user": userInfo})
	})

	log.Fatal(e.Start(":3002"))
}

// =====================================================================
// 4. Generic JWT with Redis Refresh & Revocation (Fiber)
// =====================================================================

type CustomClaims struct {
	TenantID string `json:"tenant_id"`
	jwt.RegisteredClaims
}

func RunGenericJWT_Fiber() {
	fmt.Println("--- Starting Generic JWT (Fiber) ---")

	// 1. Initialize Redis Client
	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	// 2. Initialize Generic Manager
	manager, err := auth.NewGenericManager[*CustomClaims](auth.GenericOptions{
		SecretKey: "super-secret-jwt-key-which-is-at-least-32-bytes",
		Redis:     redisClient,
	})
	if err != nil {
		log.Fatalf("Failed to initialize generic manager: %v", err)
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	// Login Endpoint
	app.Post("/login", func(c *fiber.Ctx) error {
		jti := fmt.Sprintf("jti_%d", time.Now().UnixNano())
		claims := &CustomClaims{
			TenantID: "tenant-999",
			RegisteredClaims: jwt.RegisteredClaims{
				ID:        jti,
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
			},
		}

		token, err := manager.GenerateToken(claims)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to generate token"})
		}

		ctx := context.Background()
		rt, _ := manager.GenerateRefreshToken(ctx, "user-1", 7*24*time.Hour)

		fmt.Println("Generated Generic Token:", token)
		return c.JSON(fiber.Map{
			"access_token":  token,
			"refresh_token": rt,
			"jti":           jti, // Sent for testing revocation
		})
	})

	// Logout / Revoke Endpoint
	app.Post("/logout", func(c *fiber.Ctx) error {
		jti := c.Query("jti")
		if jti == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Missing jti"})
		}

		ctx := context.Background()
		err := manager.RevokeToken(ctx, jti, 15*time.Minute)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to revoke token"})
		}
		return c.JSON(fiber.Map{"message": "Token revoked via Redis Blacklist!"})
	})

	// Check Revocation Status Endpoint
	app.Get("/check", func(c *fiber.Ctx) error {
		jti := c.Query("jti")
		ctx := context.Background()
		revoked, _ := manager.IsRevoked(ctx, jti)

		return c.JSON(fiber.Map{"jti": jti, "revoked": revoked})
	})

	// Verify Token Endpoint
	app.Post("/verify", func(c *fiber.Ctx) error {
		tokenStr := c.Query("token")
		if tokenStr == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Missing token query parameter"})
		}

		var claims CustomClaims
		if err := manager.VerifyToken(tokenStr, &claims); err != nil {
			return c.Status(401).JSON(fiber.Map{"error": "Invalid or expired token", "details": err.Error()})
		}

		// Always check if the token was blacklisted
		ctx := context.Background()
		revoked, _ := manager.IsRevoked(ctx, claims.ID)
		if revoked {
			return c.Status(401).JSON(fiber.Map{"error": "Token has been revoked"})
		}

		return c.JSON(fiber.Map{
			"message": "Token is valid!",
			"claims":  claims,
		})
	})

	// Verify Refresh Token Endpoint
	app.Post("/verify-refresh", func(c *fiber.Ctx) error {
		rt := c.Query("refresh_token")
		userID := c.Query("user_id")

		if rt == "" || userID == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Missing refresh_token or user_id query parameter"})
		}

		ctx := context.Background()
		isValid, err := manager.VerifyRefreshToken(ctx, userID, rt)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to verify refresh token", "details": err.Error()})
		}

		if !isValid {
			return c.Status(401).JSON(fiber.Map{"error": "Invalid refresh token"})
		}

		return c.JSON(fiber.Map{"message": "Refresh token is valid!"})
	})

	log.Fatal(app.Listen(":3003"))
}

// =====================================================================
// 5. Generic Encrypted JWT (AES-256 GCM) with Redis
// =====================================================================

type TenantClaims struct {
	TenantID string `json:"tenant_id"`
	Role     string `json:"role"`
	Quota    int    `json:"quota"`
}

func RunGenericEncryptedJWT_Example() {
	fmt.Println("--- Starting Generic Encrypted JWT (AES-256 GCM) ---")

	// 1. Initialize Redis Client
	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	// 2. AES Key MUST be exactly 32 bytes for AES-256 GCM
	aesKey := "01234567890123456789012345678901"

	mgr, err := auth.NewGenericEncryptedManager[TenantClaims](auth.GenericEncryptedOptions{
		SecretKey: "super-secret-jwt-key-must-be-long",
		AESKey:    aesKey,
		Redis:     redisClient,
	})
	if err != nil {
		log.Fatalf("Failed to create manager: %v", err)
	}

	payload := TenantClaims{
		TenantID: "tenant-777",
		Role:     "admin",
		Quota:    5000,
	}
	
	aad := []byte("device-id-12345") // Bind token to a specific device/context

	token, err := mgr.GenerateToken(payload, 1*time.Hour, aad)
	if err != nil {
		log.Fatalf("GenerateToken failed: %v", err)
	}
	fmt.Printf("Generated Encrypted Token: %s\n\n", token)

	// Validate token
	var decodedPayload TenantClaims
	if err := mgr.VerifyToken(token, &decodedPayload, aad); err != nil {
		log.Fatalf("VerifyToken failed: %v", err)
	}

	fmt.Printf("Decrypted Payload Successfully:\n%+v\n", decodedPayload)
}

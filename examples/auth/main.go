package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"
	
	"github.com/thanhbvha/go-common/auth"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

func main() {
	fmt.Println("=== Auth Module Examples ===")
	fmt.Println("Uncomment one of the functions below to run a specific example.")

	RunEncryptedJWT_Fiber()
	// RunStandardJWT_Gin()
	// RunStandardJWT_Echo()
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

	api.Use(auth.FiberEncryptedMiddleware(manager, aadExtractor))
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
	api.Use(auth.GinMiddleware(manager))
	
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
	api.Use(auth.EchoMiddleware(manager))
	
	api.GET("/profile", func(c echo.Context) error {
		userInfo := c.Get(string(ctxkey.UserInfo)).(*auth.UserInfo)
		return c.JSON(http.StatusOK, map[string]interface{}{"message": "Secure Echo Profile", "user": userInfo})
	})

	log.Fatal(e.Start(":3002"))
}

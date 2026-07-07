package auth

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

// GinMiddleware creates a Gin middleware to protect routes using standard JWT.
func GinMiddleware(manager *Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenString := extractTokenFromGin(c)
		if tokenString == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "Missing or invalid Authorization header",
			})
			return
		}

		userInfo, err := manager.ExtractUserInfo(tokenString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "Invalid or expired token",
			})
			return
		}

		// Store UserInfo in Gin's context
		c.Set(string(ctxkey.UserInfo), userInfo)
		c.Next()
	}
}

// GinEncryptedMiddleware creates a Gin middleware to protect routes using Encrypted JWT.
func GinEncryptedMiddleware(manager *EncryptedManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenString := extractTokenFromGin(c)
		if tokenString == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "Missing or invalid Authorization header",
			})
			return
		}

		userInfo, err := manager.ValidateToken(tokenString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "Invalid or expired token",
			})
			return
		}

		c.Set(string(ctxkey.UserInfo), userInfo)
		c.Next()
	}
}

func extractTokenFromGin(c *gin.Context) string {
	authHeader := c.GetHeader("Authorization")
	if authHeader == "" {
		return ""
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) == 2 && strings.ToLower(parts[0]) == "bearer" {
		return parts[1]
	}
	return ""
}

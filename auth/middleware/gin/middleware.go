// Package ginauth provides JWT authentication middleware for Gin.
package ginauth

import (
	"github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/auth"
	"github.com/thanhbvha/go-common/auth/middleware/internal/authhttp"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	ginresp "github.com/thanhbvha/go-common/web/response/gin"
)

// Middleware creates a Gin middleware to protect routes using standard JWT.
func Middleware(manager *auth.Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenString := extractTokenFromGin(c)
		if tokenString == "" {
			ginresp.Error(c, 401, "Missing or invalid Authorization header")
			c.Abort()
			return
		}

		userInfo, err := manager.ExtractUserInfo(tokenString)
		if err != nil {
			ginresp.Error(c, 401, "Invalid or expired token")
			c.Abort()
			return
		}

		// Store UserInfo in Gin's context
		c.Set(string(ctxkey.UserInfo), userInfo)
		c.Request = c.Request.WithContext(ctxkey.SetUserInfo(c.Request.Context(), userInfo))
		c.Next()
	}
}

// EncryptedMiddleware creates a Gin middleware to protect routes using Encrypted JWT.
// aadExtractor is an optional function to extract Dynamic AAD (e.g., from a Session ID cookie).
func EncryptedMiddleware(manager *auth.EncryptedManager, aadExtractor func(c *gin.Context) []byte) gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenString := extractTokenFromGin(c)
		if tokenString == "" {
			ginresp.Error(c, 401, "Missing or invalid Authorization header")
			c.Abort()
			return
		}

		var aad []byte
		if aadExtractor != nil {
			aad = aadExtractor(c)
		}

		userInfo, err := manager.ValidateToken(tokenString, aad)
		if err != nil {
			ginresp.Error(c, 401, "Invalid or expired token")
			c.Abort()
			return
		}

		c.Set(string(ctxkey.UserInfo), userInfo)
		c.Request = c.Request.WithContext(ctxkey.SetUserInfo(c.Request.Context(), userInfo))
		c.Next()
	}
}

func extractTokenFromGin(c *gin.Context) string {
	return authhttp.BearerToken(c.GetHeader("Authorization"))
}

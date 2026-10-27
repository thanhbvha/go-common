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

		if manager == nil {
			ginresp.Error(c, 401, "Invalid or expired token")
			c.Abort()
			return
		}
		claims, err := manager.ValidateTokenContext(c.Request.Context(), tokenString)
		if err != nil {
			ginresp.Error(c, 401, "Invalid or expired token")
			c.Abort()
			return
		}

		// Store UserInfo in Gin's context
		userInfo := &claims.UserInfo
		c.Set(string(ctxkey.UserInfo), userInfo)
		c.Set(string(ctxkey.AuthClaims), claims.RegisteredClaims)
		ctx := ctxkey.SetAuthClaims(c.Request.Context(), claims.RegisteredClaims)
		c.Request = c.Request.WithContext(ctxkey.SetUserInfo(ctx, userInfo))
		c.Next()
	}
}

// GenericMiddleware protects Gin routes with a generic token validator. The
// validated value is available from both Gin and request contexts.
func GenericMiddleware[T any](validator auth.ContextTokenValidator[T]) gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenString := extractTokenFromGin(c)
		if tokenString == "" {
			ginresp.Error(c, 401, "Missing or invalid Authorization header")
			c.Abort()
			return
		}
		if validator == nil {
			ginresp.Error(c, 401, "Invalid or expired token")
			c.Abort()
			return
		}

		claims, err := validator.ValidateRequestToken(c.Request.Context(), tokenString)
		if err != nil {
			ginresp.Error(c, 401, "Invalid or expired token")
			c.Abort()
			return
		}

		c.Set(string(ctxkey.UserInfo), claims)
		c.Set(string(ctxkey.AuthClaims), claims)
		ctx := ctxkey.SetAuthClaims(c.Request.Context(), claims)
		c.Request = c.Request.WithContext(ctxkey.SetUserInfo(ctx, claims))
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

		if manager == nil {
			ginresp.Error(c, 401, "Invalid or expired token")
			c.Abort()
			return
		}
		var aad []byte
		if aadExtractor != nil {
			aad = aadExtractor(c)
		}

		details, err := manager.ValidateTokenDetailsContext(c.Request.Context(), tokenString, aad)
		if err != nil {
			ginresp.Error(c, 401, "Invalid or expired token")
			c.Abort()
			return
		}

		userInfo := &details.UserInfo
		c.Set(string(ctxkey.UserInfo), userInfo)
		c.Set(string(ctxkey.AuthClaims), details.RegisteredClaims)
		ctx := ctxkey.SetAuthClaims(c.Request.Context(), details.RegisteredClaims)
		c.Request = c.Request.WithContext(ctxkey.SetUserInfo(ctx, userInfo))
		c.Next()
	}
}

func extractTokenFromGin(c *gin.Context) string {
	return authhttp.BearerToken(c.GetHeader("Authorization"))
}

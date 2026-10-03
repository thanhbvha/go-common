package health

import "github.com/gin-gonic/gin"

// GinHandler handles GET /health for Gin.
func GinHandler(c *Checker) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		r := c.CheckHealth(ctx.Request.Context())
		ctx.JSON(statusCode(r), r)
	}
}

// GinReadyHandler handles GET /ready for Gin.
func GinReadyHandler(c *Checker) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		r := c.CheckReadiness(ctx.Request.Context())
		ctx.JSON(statusCode(r), r)
	}
}

// GinLiveHandler handles GET /live for Gin.
func GinLiveHandler(c *Checker) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		r := c.CheckLiveness(ctx.Request.Context())
		ctx.JSON(statusCode(r), r)
	}
}

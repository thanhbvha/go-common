// Package gin provides an adapter to bridge the Gin web framework and our generic WebSocket core.
package gin

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/websocket/limiter"
	"github.com/thanhbvha/go-common/websocket/pubsub"
	"github.com/thanhbvha/go-common/websocket/core"
)

// Config holds the configuration options for the Gin WebSocket adapter.
type Config struct {
	// Authenticate resolves user identifiers from the Gin context.
	// Returns the UserID, or an error if unauthorized.
	Authenticate func(c *gin.Context) (string, error)
	// Upgrader defines the websocket Upgrade configuration. If nil, standard Upgrader is used.
	Upgrader *websocket.Upgrader
	// RateLimiter specifies a custom RateLimiter. Falls back to global instance if nil.
	RateLimiter *limiter.RateLimiter
	// ConnectionLimiter specifies a custom ConnectionLimiter. Falls back to global instance if nil.
	ConnectionLimiter *limiter.ConnectionLimiter
}

// Handler manages the Gin HTTP endpoints and upgrades connections to the core WebSocket manager.
type Handler struct {
	config Config
}

// NewHandler instantiates a new Gin adapter Handler. It accepts optional variadic Config options,
// merging any provided configurations with the library's defaults.
func NewHandler(customConfig ...Config) *Handler {
	var config Config
	if len(customConfig) > 0 {
		config = customConfig[0]
	}

	if config.Authenticate == nil {
		config.Authenticate = func(c *gin.Context) (string, error) {
			token := c.Query("token")
			if token == "" {
				return "", fmt.Errorf("token query parameter is required")
			}
			return token, nil
		}
	}
	if config.Upgrader == nil {
		config.Upgrader = &websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		}
	}
	if config.RateLimiter == nil {
		config.RateLimiter = limiter.GetGlobalRateLimiter()
	}
	if config.ConnectionLimiter == nil {
		config.ConnectionLimiter = limiter.GetGlobalConnectionLimiter()
	}

	return &Handler{config: config}
}

// HandleUpgrade handles the Gin WebSocket connection upgrade process and registers session inside the core manager.
func (h *Handler) HandleUpgrade(c *gin.Context) {
	userID, err := h.config.Authenticate(c)
	if err != nil || userID == "" {
		logger.WarnAsync("WebSocket connection upgrade unauthorized", "error", err)
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}

	clientIP := c.ClientIP()
	requestID := c.Writer.Header().Get("X-Request-Id")

	// Apply Rate Limiters
	if !h.config.RateLimiter.Allow() {
		logger.WarnAsync("Connection upgrade rejected due to rate limiting", "clientIP", clientIP, "userID", userID)
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
		return
	}

	if !h.config.ConnectionLimiter.CanConnect(clientIP) {
		logger.WarnAsync("Connection upgrade rejected due to key connection limit", "clientIP", clientIP, "userID", userID)
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "connection limit exceeded"})
		return
	}

	// Upgrade the connection
	conn, err := h.config.Upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		logger.ErrorAsync("Failed to upgrade Gin HTTP request to WebSocket", "error", err, "userID", userID)
		return
	}

	if !h.config.ConnectionLimiter.AddConnection(clientIP) {
		_ = conn.Close()
		return
	}

	go func() {
		defer h.config.ConnectionLimiter.RemoveConnection(clientIP)
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorAsync("Panic in Gin connection handler loop", "error", r, "userID", userID)
			}
		}()

		manager := core.GetGlobalManager()
		shardID := manager.GetShardID(userID)

		if err := manager.HandleConnection(conn, shardID, userID, clientIP, requestID); err != nil {
			logger.ErrorAsync("Failed to handle WebSocket connection in manager", "error", err, "userID", userID)
		}
	}()
}

// HandleStats returns JSON-formatted metrics of the Manager, PubSub, and limiters.
func (h *Handler) HandleStats(c *gin.Context) {
	manager := core.GetGlobalManager()
	pubsubManager := pubsub.GetGlobalPubSub()

	c.JSON(http.StatusOK, gin.H{
		"manager":      manager.GetStats(),
		"rateLimiter":  h.config.ConnectionLimiter.GetStats(),
		"pubsub":       pubsubManager.GetStats(),
		"timestamp":    time.Now(),
	})
}

// HandleShardManagement retrieves statistics or checks status of a specific Shard.
func (h *Handler) HandleShardManagement(c *gin.Context) {
	shardID := c.Query("shard")
	if shardID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "shard ID required"})
		return
	}

	manager := core.GetGlobalManager()
	if shard, exists := manager.GetShard(shardID); exists {
		c.JSON(http.StatusOK, gin.H{
			"shardID":     shardID,
			"connections": shard.GetConnectionCount(),
			"exists":      true,
		})
		return
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "shard not found"})
}

// HandleHealthCheck reports overall health status of the connection Manager and cluster node configuration.
func (h *Handler) HandleHealthCheck(c *gin.Context) {
	manager := core.GetGlobalManager()
	stats := manager.GetStats()

	c.JSON(http.StatusOK, gin.H{
		"status":           "healthy",
		"timestamp":        time.Now(),
		"totalConnections": stats["totalConnections"],
		"totalShards":      stats["totalShards"],
		"memoryLimit":      stats["maxTotalConnections"],
		"shardLimit":       stats["maxShards"],
		"nodeID":           pubsub.GetGlobalPubSub().GetNodeID(),
	})
}

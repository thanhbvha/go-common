// Package echo provides an adapter to bridge the Echo web framework and our generic WebSocket core.
package echo

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/websocket/limiter"
	"github.com/thanhbvha/go-common/websocket/pubsub"
	"github.com/thanhbvha/go-common/websocket/core"
)

// Config holds the configuration options for the Echo WebSocket adapter.
type Config struct {
	// Authenticate resolves user identifiers from the Echo context.
	// Returns the UserID, or an error if unauthorized.
	Authenticate func(c echo.Context) (string, error)
	// Upgrader defines the websocket Upgrade configuration. If nil, standard Upgrader is used.
	Upgrader *websocket.Upgrader
	// RateLimiter specifies a custom RateLimiter. Falls back to global instance if nil.
	RateLimiter *limiter.RateLimiter
	// ConnectionLimiter specifies a custom ConnectionLimiter. Falls back to global instance if nil.
	ConnectionLimiter *limiter.ConnectionLimiter
	// PubSubAdapter specifies the pubsub backend to use ("redis" or "nats"). Defaults to "redis".
	PubSubAdapter string
}

// Handler manages the Echo HTTP endpoints and upgrades connections to the core WebSocket manager.
type Handler struct {
	config Config
}

// NewHandler instantiates a new Echo adapter Handler. It accepts optional variadic Config options,
// merging any provided configurations with the library's defaults.
func NewHandler(customConfig ...Config) *Handler {
	var config Config
	if len(customConfig) > 0 {
		config = customConfig[0]
	}

	if config.Authenticate == nil {
		config.Authenticate = func(c echo.Context) (string, error) {
			token := c.QueryParam("token")
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
	if config.PubSubAdapter == "" {
		config.PubSubAdapter = pubsub.AdapterRedis
	}

	// Pre-initialize the core global manager with the specified pubsub adapter
	core.GetGlobalManager(config.PubSubAdapter)

	return &Handler{config: config}
}

// HandleUpgrade handles the Echo WebSocket connection upgrade process and registers session inside the core manager.
func (h *Handler) HandleUpgrade(c echo.Context) error {
	userID, err := h.config.Authenticate(c)
	if err != nil || userID == "" {
		logger.WarnAsync("WebSocket connection upgrade unauthorized", "error", err)
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
	}

	clientIP := c.RealIP()
	requestID := c.Response().Header().Get(echo.HeaderXRequestID)

	// Apply Rate Limiters
	if !h.config.RateLimiter.Allow() {
		logger.WarnAsync("Connection upgrade rejected due to rate limiting", "clientIP", clientIP, "userID", userID)
		return c.JSON(http.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded"})
	}

	if !h.config.ConnectionLimiter.CanConnect(clientIP) {
		logger.WarnAsync("Connection upgrade rejected due to key connection limit", "clientIP", clientIP, "userID", userID)
		return c.JSON(http.StatusTooManyRequests, map[string]string{"error": "connection limit exceeded"})
	}

	// Upgrade the connection
	conn, err := h.config.Upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		logger.ErrorAsync("Failed to upgrade Echo HTTP request to WebSocket", "error", err, "userID", userID)
		return nil
	}

	if !h.config.ConnectionLimiter.AddConnection(clientIP) {
		_ = conn.Close()
		return nil
	}

	go func() {
		defer h.config.ConnectionLimiter.RemoveConnection(clientIP)
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorAsync("Panic in Echo connection handler loop", "error", r, "userID", userID)
			}
		}()

		manager := core.GetGlobalManager()
		shardID := manager.GetShardID(userID)

		if err := manager.HandleConnection(conn, shardID, userID, clientIP, requestID); err != nil {
			logger.ErrorAsync("Failed to handle WebSocket connection in manager", "error", err, "userID", userID)
		}
	}()

	return nil
}

// HandleStats returns JSON-formatted metrics of the Manager, PubSub, and limiters.
func (h *Handler) HandleStats(c echo.Context) error {
	manager := core.GetGlobalManager()
	pubsubManager := manager.GetPubSubManager()

	return c.JSON(http.StatusOK, map[string]interface{}{
		"manager":      manager.GetStats(),
		"rateLimiter":  h.config.ConnectionLimiter.GetStats(),
		"pubsub":       pubsubManager.GetStats(),
		"timestamp":    time.Now(),
	})
}

// HandleShardManagement retrieves statistics or checks status of a specific Shard.
func (h *Handler) HandleShardManagement(c echo.Context) error {
	shardID := c.QueryParam("shard")
	if shardID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "shard ID required"})
	}

	manager := core.GetGlobalManager()
	if shard, exists := manager.GetShard(shardID); exists {
		return c.JSON(http.StatusOK, map[string]interface{}{
			"shardID":     shardID,
			"connections": shard.GetConnectionCount(),
			"exists":      true,
		})
	}

	return c.JSON(http.StatusNotFound, map[string]string{"error": "shard not found"})
}

// HandleHealthCheck reports overall health status of the connection Manager and cluster node configuration.
func (h *Handler) HandleHealthCheck(c echo.Context) error {
	manager := core.GetGlobalManager()
	stats := manager.GetStats()

	return c.JSON(http.StatusOK, map[string]interface{}{
		"status":           "healthy",
		"timestamp":        time.Now(),
		"totalConnections": stats["totalConnections"],
		"totalShards":      stats["totalShards"],
		"memoryLimit":      stats["maxTotalConnections"],
		"shardLimit":       stats["maxShards"],
		"nodeID":           manager.GetPubSubManager().GetNodeID(),
	})
}

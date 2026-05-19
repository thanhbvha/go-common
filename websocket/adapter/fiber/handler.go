// Package fiber provides an adapter to bridge the Fiber web framework and our generic WebSocket core.
package fiber

import (
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/websocket/limiter"
	"github.com/thanhbvha/go-common/websocket/pubsub"
	"github.com/thanhbvha/go-common/websocket/ws"
)

// Config holds the configuration options for the Fiber WebSocket adapter.
type Config struct {
	// Authenticate resolves user identifiers from the Fiber context.
	// Returns the UserID, or an error if unauthorized.
	Authenticate func(c *fiber.Ctx) (string, error)
	// RateLimiter specifies a custom RateLimiter. Falls back to global instance if nil.
	RateLimiter *limiter.RateLimiter
	// ConnectionLimiter specifies a custom ConnectionLimiter. Falls back to global instance if nil.
	ConnectionLimiter *limiter.ConnectionLimiter
}

// Handler manages the Fiber HTTP endpoints and upgrades connections to the core WebSocket manager.
type Handler struct {
	config Config
}

// NewHandler instantiates a new Fiber adapter Handler with the specified configuration.
func NewHandler(config Config) *Handler {
	if config.Authenticate == nil {
		// Provide a default query token authenticator if none is provided
		config.Authenticate = func(c *fiber.Ctx) (string, error) {
			token := c.Query("token")
			if token == "" {
				return "", fmt.Errorf("token query parameter is required")
			}
			// In production, user would parse and validate token here
			return token, nil
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

// HandleUpgrade handles the Fiber WebSocket connection upgrade process and registers session wrapper inside the core manager.
func (h *Handler) HandleUpgrade(c *fiber.Ctx) error {
	if !websocket.IsWebSocketUpgrade(c) {
		logger.WarnAsync("Received non-websocket upgrade HTTP request")
		return c.Status(fiber.StatusUpgradeRequired).JSON(fiber.Map{"error": "upgrade required"})
	}

	userID, err := h.config.Authenticate(c)
	if err != nil || userID == "" {
		logger.WarnAsync("WebSocket connection upgrade unauthorized", "error", err)
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "unauthorized"})
	}

	clientIP := c.IP()
	requestID := ""
	if reqIDVal := c.Locals("requestid"); reqIDVal != nil {
		if reqIDStr, ok := reqIDVal.(string); ok {
			requestID = reqIDStr
		}
	}

	return websocket.New(func(conn *websocket.Conn) {
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorAsync("Panic in Fiber connection handler loop", "error", r, "userID", userID)
			}
		}()

		if conn == nil {
			return
		}

		if !h.config.RateLimiter.Allow() {
			logger.WarnAsync("Connection upgrade rejected due to rate limiting", "clientIP", clientIP, "userID", userID)
			_ = conn.Close()
			return
		}

		if !h.config.ConnectionLimiter.CanConnect(clientIP) {
			logger.WarnAsync("Connection upgrade rejected due to key connection limit", "clientIP", clientIP, "userID", userID)
			_ = conn.Close()
			return
		}

		if !h.config.ConnectionLimiter.AddConnection(clientIP) {
			_ = conn.Close()
			return
		}

		defer h.config.ConnectionLimiter.RemoveConnection(clientIP)

		manager := ws.GetGlobalManager()
		shardID := manager.GetShardID(userID)

		adapterConn := NewConnAdapter(conn)

		if err := manager.HandleConnection(adapterConn, shardID, userID, clientIP, requestID); err != nil {
			logger.ErrorAsync("Failed to handle WebSocket connection in manager", "error", err, "userID", userID)
		}
	})(c)
}

// HandleStats returns JSON-formatted runtime metrics of the Manager, pubsub system, and limiters.
func (h *Handler) HandleStats(c *fiber.Ctx) error {
	manager := ws.GetGlobalManager()
	pubsubManager := pubsub.GetGlobalPubSub()

	return c.JSON(fiber.Map{
		"manager":      manager.GetStats(),
		"rateLimiter":  h.config.ConnectionLimiter.GetStats(),
		"pubsub":       pubsubManager.GetStats(),
		"timestamp":    time.Now(),
	})
}

// HandleShardManagement retrieves statistics or checks status of a specific Shard.
func (h *Handler) HandleShardManagement(c *fiber.Ctx) error {
	shardID := c.Query("shard")
	if shardID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "shard ID required"})
	}

	manager := ws.GetGlobalManager()
	if shard, exists := manager.GetShard(shardID); exists {
		return c.JSON(fiber.Map{
			"shardID":     shardID,
			"connections": shard.GetConnectionCount(),
			"exists":      true,
		})
	}

	return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "shard not found"})
}

// HandleHealthCheck reports overall health status of the connection Manager and cluster node configuration.
func (h *Handler) HandleHealthCheck(c *fiber.Ctx) error {
	manager := ws.GetGlobalManager()
	stats := manager.GetStats()

	return c.JSON(fiber.Map{
		"status":           "healthy",
		"timestamp":        time.Now(),
		"totalConnections": stats["totalConnections"],
		"totalShards":      stats["totalShards"],
		"memoryLimit":      stats["maxTotalConnections"],
		"shardLimit":       stats["maxShards"],
		"nodeID":           pubsub.GetGlobalPubSub().GetNodeID(),
	})
}

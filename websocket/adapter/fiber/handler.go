// Package fiber provides an adapter to bridge the Fiber web framework and our generic WebSocket core.
package fiber

import (
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/thanhbvha/go-common/logger"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/websocket/adapter/internal/wshttp"
	"github.com/thanhbvha/go-common/websocket/core"
	"github.com/thanhbvha/go-common/websocket/internal/wsrecovery"
	"github.com/thanhbvha/go-common/websocket/limiter"
)

// Config holds the configuration options for the Fiber WebSocket adapter.
type Config struct {
	// Manager is the core manager used by this adapter.
	Manager *core.Manager
	// Authenticate resolves user identifiers from the Fiber context.
	// Returns the UserID, or an error if unauthorized.
	Authenticate func(c *fiber.Ctx) (string, error)
	// AuthenticateIdentity resolves a structured identity and takes precedence over Authenticate.
	AuthenticateIdentity func(c *fiber.Ctx) (core.Identity, error)
	// CheckOrigin validates the browser Origin before upgrading. The default
	// allows requests without Origin and same-origin requests only.
	CheckOrigin func(c *fiber.Ctx) bool
	// RateLimiter specifies a custom RateLimiter. Falls back to global instance if nil.
	RateLimiter *limiter.RateLimiter
	// ConnectionLimiter specifies a custom ConnectionLimiter. Falls back to global instance if nil.
	ConnectionLimiter *limiter.ConnectionLimiter
	// ConnectionKey resolves the key used for concurrent connection limits and
	// connection metadata. Configure Fiber trusted proxies before using c.IP().
	ConnectionKey func(c *fiber.Ctx, userID string) string
}

// Handler manages the Fiber HTTP endpoints and upgrades connections to the core WebSocket manager.
type Handler struct {
	config  Config
	manager *core.Manager
}

// NewHandler instantiates a new Fiber adapter Handler with the specified configuration.
func NewHandler(config Config) *Handler {
	if config.Authenticate == nil {
		config.Authenticate = func(*fiber.Ctx) (string, error) {
			return "", fmt.Errorf("websocket authenticator is required")
		}
	}
	if config.RateLimiter == nil {
		config.RateLimiter = limiter.GetGlobalRateLimiter()
	}
	if config.ConnectionLimiter == nil {
		config.ConnectionLimiter = limiter.GetGlobalConnectionLimiter()
	}
	if config.ConnectionKey == nil {
		config.ConnectionKey = func(c *fiber.Ctx, _ string) string { return c.IP() }
	}
	if config.CheckOrigin == nil {
		config.CheckOrigin = func(c *fiber.Ctx) bool {
			return wshttp.CheckSameOrigin(c.Get(fiber.HeaderOrigin), string(c.Context().Host()))
		}
	}

	if config.Manager == nil {
		config.Manager = core.GetGlobalManager()
	}
	return &Handler{config: config, manager: config.Manager}
}

// HandleUpgrade handles the Fiber WebSocket connection upgrade process and registers session wrapper inside the core manager.
func (h *Handler) HandleUpgrade(c *fiber.Ctx) error {
	if !h.manager.IsAcceptingConnections() {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "service draining"})
	}
	if !websocket.IsWebSocketUpgrade(c) {
		logger.WarnAsync("Received non-websocket upgrade HTTP request")
		return c.Status(fiber.StatusUpgradeRequired).JSON(fiber.Map{"error": "upgrade required"})
	}
	if !h.config.CheckOrigin(c) {
		logger.WarnAsync("WebSocket connection upgrade rejected due to origin policy", "origin", c.Get(fiber.HeaderOrigin))
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "origin not allowed"})
	}

	identity := core.Identity{}
	var err error
	if h.config.AuthenticateIdentity != nil {
		identity, err = h.config.AuthenticateIdentity(c)
	} else {
		identity.UserID, err = h.config.Authenticate(c)
	}
	if err != nil || identity.UserID == "" {
		logger.WarnAsync("WebSocket connection upgrade unauthorized", "error", err)
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "unauthorized"})
	}
	userID := identity.UserID

	clientIP := h.config.ConnectionKey(c, userID)
	requestID := getRequestID(c)

	if !h.config.RateLimiter.Allow() {
		logger.WarnAsync("Connection upgrade rejected due to rate limiting", "clientIP", clientIP, "userID", userID)
		return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{"error": "rate limit exceeded"})
	}
	if !h.config.ConnectionLimiter.CanConnect(clientIP) {
		logger.WarnAsync("Connection upgrade rejected due to key connection limit", "clientIP", clientIP, "userID", userID)
		return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{"error": "connection limit exceeded"})
	}

	return websocket.New(func(conn *websocket.Conn) {
		if conn == nil {
			return
		}
		defer func() { _ = conn.Close() }()
		defer panicrecovery.Catch(wsrecovery.Handler(wsrecovery.Metadata{
			Component: "adapter/fiber.connection",
			UserID:    userID,
			ClientIP:  clientIP,
			RequestID: requestID,
		}))

		if !h.config.ConnectionLimiter.AddConnection(clientIP) {
			_ = conn.Close()
			return
		}

		defer h.config.ConnectionLimiter.RemoveConnection(clientIP)

		shardID := h.manager.GetShardID(userID)

		adapterConn := NewConnAdapter(conn)

		if err := h.manager.HandleIdentityConnection(adapterConn, shardID, identity, clientIP, requestID); err != nil {
			logger.ErrorAsync("Failed to handle WebSocket connection in manager", "error", err, "userID", userID)
		}
	})(c)
}

func getRequestID(c *fiber.Ctx) string {
	if requestID, ok := ctxkey.GetRequestID(c.UserContext()); ok {
		return requestID
	}
	if requestID, ok := c.Locals("request_id").(string); ok {
		return requestID
	}
	return c.Get("X-Request-ID")
}

// HandleStats returns JSON-formatted runtime metrics of the Manager, pubsub system, and limiters.
func (h *Handler) HandleStats(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"manager":     h.manager.GetStats(),
		"rateLimiter": h.config.ConnectionLimiter.GetStats(),
		"pubsub":      h.manager.GetPubSubManager().GetStats(),
		"timestamp":   time.Now(),
	})
}

// HandleShardManagement retrieves statistics or checks status of a specific Shard.
func (h *Handler) HandleShardManagement(c *fiber.Ctx) error {
	shardID := c.Query("shard")
	if shardID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "shard ID required"})
	}

	if shard, exists := h.manager.GetShard(shardID); exists {
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
	readiness := h.manager.Readiness()
	status := fiber.StatusOK
	if !readiness.Ready {
		status = fiber.StatusServiceUnavailable
	}
	return c.Status(status).JSON(fiber.Map{"status": map[bool]string{true: "ready", false: "not_ready"}[readiness.Ready], "checks": readiness})
}

// Package gin provides an adapter to bridge the Gin web framework and our generic WebSocket core.
package gin

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/thanhbvha/go-common/logger"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/websocket/core"
	"github.com/thanhbvha/go-common/websocket/internal/wsrecovery"
	"github.com/thanhbvha/go-common/websocket/limiter"
)

// Config holds the configuration options for the Gin WebSocket adapter.
type Config struct {
	// Manager is the core manager used by this adapter.
	Manager *core.Manager
	// Authenticate resolves user identifiers from the Gin context.
	// Returns the UserID, or an error if unauthorized.
	Authenticate func(c *gin.Context) (string, error)
	// AuthenticateIdentity resolves a structured identity and takes precedence over Authenticate.
	AuthenticateIdentity func(c *gin.Context) (core.Identity, error)
	// Upgrader defines the websocket Upgrade configuration. If nil, standard Upgrader is used.
	Upgrader *websocket.Upgrader
	// RateLimiter specifies a custom RateLimiter. Falls back to global instance if nil.
	RateLimiter *limiter.RateLimiter
	// ConnectionLimiter specifies a custom ConnectionLimiter. Falls back to global instance if nil.
	ConnectionLimiter *limiter.ConnectionLimiter
	// ConnectionKey resolves the key used for concurrent connection limits and
	// connection metadata. It may return a trusted client IP or account key.
	ConnectionKey func(c *gin.Context, userID string) string
}

// Handler manages the Gin HTTP endpoints and upgrades connections to the core WebSocket manager.
type Handler struct {
	config  Config
	manager *core.Manager
}

// NewHandler instantiates a new Gin adapter Handler. It accepts optional variadic Config options,
// merging any provided configurations with the library's defaults.
func NewHandler(customConfig ...Config) *Handler {
	var config Config
	if len(customConfig) > 0 {
		config = customConfig[0]
	}

	if config.Authenticate == nil {
		config.Authenticate = func(*gin.Context) (string, error) {
			return "", fmt.Errorf("websocket authenticator is required")
		}
	}
	if config.Upgrader == nil {
		config.Upgrader = &websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		}
	}
	if config.RateLimiter == nil {
		config.RateLimiter = limiter.GetGlobalRateLimiter()
	}
	if config.ConnectionLimiter == nil {
		config.ConnectionLimiter = limiter.GetGlobalConnectionLimiter()
	}
	if config.ConnectionKey == nil {
		config.ConnectionKey = func(c *gin.Context, _ string) string { return c.ClientIP() }
	}

	if config.Manager == nil {
		config.Manager = core.GetGlobalManager()
	}
	return &Handler{config: config, manager: config.Manager}
}

// HandleUpgrade handles the Gin WebSocket connection upgrade process and registers session inside the core manager.
func (h *Handler) HandleUpgrade(c *gin.Context) {
	if !h.manager.IsAcceptingConnections() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "service draining"})
		return
	}
	if !websocket.IsWebSocketUpgrade(c.Request) {
		c.JSON(http.StatusUpgradeRequired, gin.H{"error": "upgrade required"})
		return
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
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	userID := identity.UserID

	clientIP := h.config.ConnectionKey(c, userID)
	requestID := getRequestID(c)

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
		defer func() { _ = conn.Close() }()
		defer panicrecovery.Catch(wsrecovery.Handler(wsrecovery.Metadata{
			Component: "adapter/gin.connection",
			UserID:    userID,
			ClientIP:  clientIP,
			RequestID: requestID,
		}))

		shardID := h.manager.GetShardID(userID)

		if err := h.manager.HandleIdentityConnection(conn, shardID, identity, clientIP, requestID); err != nil {
			logger.ErrorAsync("Failed to handle WebSocket connection in manager", "error", err, "userID", userID)
		}
	}()
}

func getRequestID(c *gin.Context) string {
	if requestID, ok := ctxkey.GetRequestID(c.Request.Context()); ok {
		return requestID
	}
	if value, exists := c.Get("request_id"); exists {
		if requestID, ok := value.(string); ok {
			return requestID
		}
	}
	return c.Writer.Header().Get("X-Request-ID")
}

// HandleStats returns JSON-formatted metrics of the Manager, PubSub, and limiters.
func (h *Handler) HandleStats(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"manager":     h.manager.GetStats(),
		"rateLimiter": h.config.ConnectionLimiter.GetStats(),
		"pubsub":      h.manager.GetPubSubManager().GetStats(),
		"timestamp":   time.Now(),
	})
}

// HandleShardManagement retrieves statistics or checks status of a specific Shard.
func (h *Handler) HandleShardManagement(c *gin.Context) {
	shardID := c.Query("shard")
	if shardID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "shard ID required"})
		return
	}

	if shard, exists := h.manager.GetShard(shardID); exists {
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
	readiness := h.manager.Readiness()
	status := http.StatusOK
	if !readiness.Ready {
		status = http.StatusServiceUnavailable
	}
	c.JSON(status, gin.H{"status": map[bool]string{true: "ready", false: "not_ready"}[readiness.Ready], "checks": readiness})
}

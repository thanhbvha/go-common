// Package echo provides an adapter to bridge the Echo web framework and our generic WebSocket core.
package echo

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/v2/internal/httpkit"
	"github.com/thanhbvha/go-common/v2/logger"
	panicrecovery "github.com/thanhbvha/go-common/v2/recovery"
	"github.com/thanhbvha/go-common/v2/utils/ctxkey"
	"github.com/thanhbvha/go-common/v2/websocket/adapter/internal/wshttp"
	"github.com/thanhbvha/go-common/v2/websocket/core"
	"github.com/thanhbvha/go-common/v2/websocket/internal/wsrecovery"
	"github.com/thanhbvha/go-common/v2/websocket/limiter"
)

// Config holds the configuration options for the Echo WebSocket adapter.
type Config struct {
	// Manager is the core manager used by this adapter.
	Manager *core.Manager
	// Authenticate resolves user identifiers from the Echo context.
	// Returns the UserID, or an error if unauthorized.
	Authenticate func(c echo.Context) (string, error)
	// AuthenticateIdentity resolves a structured identity and takes precedence over Authenticate.
	AuthenticateIdentity func(c echo.Context) (core.Identity, error)
	// Upgrader defines the websocket Upgrade configuration. If nil, standard Upgrader is used.
	Upgrader *websocket.Upgrader
	// IPRateLimiter limits upgrade attempts per direct peer IP before the
	// request is authenticated. The default allows wshttp.DefaultIPRate/Burst.
	IPRateLimiter *limiter.KeyedRateLimiter
	// ConnectionLimiter specifies a custom ConnectionLimiter. Falls back to global instance if nil.
	ConnectionLimiter *limiter.ConnectionLimiter
	// ConnectionKey resolves the key used for concurrent connection limits and
	// connection metadata. The default is the direct network peer IP and never
	// trusts forwarding headers; behind a trusted proxy, return a verified client IP.
	ConnectionKey func(c echo.Context, userID string) string
}

// Handler manages the Echo HTTP endpoints and upgrades connections to the core WebSocket manager.
type Handler struct {
	config  Config
	manager *core.Manager
	gate    wshttp.Gate
}

// NewHandler instantiates a new Echo adapter Handler. It accepts optional variadic Config options,
// merging any provided configurations with the library's defaults.
func NewHandler(customConfig ...Config) *Handler {
	var config Config
	if len(customConfig) > 0 {
		config = customConfig[0]
	}

	if config.Authenticate == nil {
		config.Authenticate = func(echo.Context) (string, error) {
			return "", fmt.Errorf("websocket authenticator is required")
		}
	}
	if config.Upgrader == nil {
		config.Upgrader = &websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		}
	}
	if config.ConnectionLimiter == nil {
		config.ConnectionLimiter = limiter.NewConnectionLimiter(100)
	}
	if config.ConnectionKey == nil {
		config.ConnectionKey = func(c echo.Context, _ string) string { return httpkit.ClientIP(c.Request()) }
	}

	if config.Manager == nil {
		config.Manager = core.NewManager()
	}
	return &Handler{config: config, manager: config.Manager, gate: wshttp.NewGate(config.Manager, config.IPRateLimiter)}
}

// HandleUpgrade handles the Echo WebSocket connection upgrade process and registers session inside the core manager.
func (h *Handler) HandleUpgrade(c echo.Context) error {
	if denial := h.gate.Admit(httpkit.ClientIP(c.Request())); denial != nil {
		c.Response().Header().Set("Retry-After", denial.RetryAfterHeader())
		return c.JSON(denial.Status, map[string]string{"error": denial.Message})
	}
	if !websocket.IsWebSocketUpgrade(c.Request()) {
		return c.JSON(http.StatusUpgradeRequired, map[string]string{"error": "upgrade required"})
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
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
	}
	userID := identity.UserID

	if denial := h.gate.AdmitIdentity(identity); denial != nil {
		c.Response().Header().Set("Retry-After", denial.RetryAfterHeader())
		return c.JSON(denial.Status, map[string]string{"error": denial.Message})
	}

	clientIP := h.config.ConnectionKey(c, userID)
	requestID := getRequestID(c)

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
		defer func() { _ = conn.Close() }()
		defer panicrecovery.Catch(wsrecovery.Handler(wsrecovery.Metadata{
			Component: "adapter/echo.connection",
			UserID:    userID,
			ClientIP:  clientIP,
			RequestID: requestID,
		}))

		shardID := h.manager.GetShardID(userID)

		if err := h.manager.HandleIdentityConnection(conn, shardID, identity, clientIP, requestID); err != nil {
			logger.ErrorAsync("Failed to handle WebSocket connection in manager", "error", err, "userID", userID)
		}
	}()

	return nil
}

func getRequestID(c echo.Context) string {
	if requestID, ok := ctxkey.GetRequestID(c.Request().Context()); ok {
		return requestID
	}
	if value := c.Get("request_id"); value != nil {
		if requestID, ok := value.(string); ok {
			return requestID
		}
	}
	return c.Response().Header().Get(echo.HeaderXRequestID)
}

// HandleStats returns JSON-formatted metrics of the Manager, PubSub, and limiters.
func (h *Handler) HandleStats(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]interface{}{
		"manager":     h.manager.GetStats(),
		"rateLimiter": h.config.ConnectionLimiter.GetStats(),
		"pubsub":      h.manager.GetPubSubManager().GetStats(),
		"timestamp":   time.Now(),
	})
}

// HandleShardManagement retrieves statistics or checks status of a specific Shard.
func (h *Handler) HandleShardManagement(c echo.Context) error {
	shardID := c.QueryParam("shard")
	if shardID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "shard ID required"})
	}

	if shard, exists := h.manager.GetShard(shardID); exists {
		return c.JSON(http.StatusOK, map[string]interface{}{
			"shardID":     shardID,
			"connections": shard.GetConnectionCount(),
			"exists":      true,
		})
	}

	return c.JSON(http.StatusNotFound, map[string]string{"error": "shard not found"})
}

// HandleLive is the liveness probe: 200 whenever the process can answer. It
// never consults the manager or a broker, so a broker outage or a draining node
// does not get a healthy process restarted. Mount it at /livez.
func (h *Handler) HandleLive(c echo.Context) error {
	status, body := wshttp.LiveStatus()
	return c.JSON(status, body)
}

// HandleReady is the readiness probe: 200 only while this node should receive
// new connections, otherwise 503. Mount it at /readyz. It reveals internal state
// (broker and executor status), so serve it on an internal listener.
func (h *Handler) HandleReady(c echo.Context) error {
	status, body := wshttp.ReadyStatus(h.manager.Readiness())
	return c.JSON(status, body)
}

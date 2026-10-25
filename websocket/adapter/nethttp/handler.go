// Package nethttp provides a WebSocket adapter for the standard library's
// net/http package.
package nethttp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/thanhbvha/go-common/internal/httpkit"
	"github.com/thanhbvha/go-common/logger"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/websocket/core"
	"github.com/thanhbvha/go-common/websocket/internal/wsrecovery"
	"github.com/thanhbvha/go-common/websocket/limiter"
)

// Config holds configuration for the standard net/http WebSocket adapter.
type Config struct {
	// Manager is the core manager used by this adapter.
	Manager *core.Manager
	// Authenticate resolves a user identifier from an upgrade request.
	Authenticate func(*http.Request) (string, error)
	// AuthenticateIdentity resolves a structured identity and takes precedence over Authenticate.
	AuthenticateIdentity func(*http.Request) (core.Identity, error)
	// Upgrader controls the WebSocket handshake. The default enforces same-origin.
	Upgrader *websocket.Upgrader
	// RateLimiter limits connection upgrade attempts.
	RateLimiter *limiter.RateLimiter
	// ConnectionLimiter limits concurrent connections per direct peer IP.
	ConnectionLimiter *limiter.ConnectionLimiter
	// ConnectionKey resolves the key used for concurrent connection limits and
	// connection metadata. The default uses the direct network peer IP.
	ConnectionKey func(*http.Request, string) string
}

// Handler serves WebSocket upgrade and operational HTTP endpoints.
type Handler struct {
	config  Config
	manager *core.Manager
}

// NewHandler creates a standard-library WebSocket handler. Cross-origin use
// requires an explicit Upgrader with a carefully validated CheckOrigin policy.
func NewHandler(customConfig ...Config) *Handler {
	var config Config
	if len(customConfig) > 0 {
		config = customConfig[0]
	}
	if config.Authenticate == nil {
		config.Authenticate = func(*http.Request) (string, error) {
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
		config.ConnectionKey = func(r *http.Request, _ string) string { return httpkit.ClientIP(r) }
	}

	if config.Manager == nil {
		config.Manager = core.GetGlobalManager()
	}
	return &Handler{config: config, manager: config.Manager}
}

// HandleUpgrade authenticates, limits, and upgrades an HTTP request before
// registering the connection with the core manager.
func (h *Handler) HandleUpgrade(w http.ResponseWriter, r *http.Request) {
	if !h.manager.IsAcceptingConnections() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "service draining"})
		return
	}
	if !websocket.IsWebSocketUpgrade(r) {
		writeJSON(w, http.StatusUpgradeRequired, map[string]string{"error": "upgrade required"})
		return
	}

	identity := core.Identity{}
	var err error
	if h.config.AuthenticateIdentity != nil {
		identity, err = h.config.AuthenticateIdentity(r)
	} else {
		identity.UserID, err = h.config.Authenticate(r)
	}
	if err != nil || identity.UserID == "" {
		logger.WarnAsync("WebSocket connection upgrade unauthorized", "error", err)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	userID := identity.UserID

	clientIP := h.config.ConnectionKey(r, userID)
	if !h.config.RateLimiter.Allow() {
		logger.WarnAsync("Connection upgrade rejected due to rate limiting", "clientIP", clientIP, "userID", userID)
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded"})
		return
	}
	if !h.config.ConnectionLimiter.CanConnect(clientIP) {
		logger.WarnAsync("Connection upgrade rejected due to key connection limit", "clientIP", clientIP, "userID", userID)
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "connection limit exceeded"})
		return
	}

	conn, err := h.config.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.WarnAsync("Failed to upgrade net/http request to WebSocket", "error", err, "userID", userID)
		return
	}
	if !h.config.ConnectionLimiter.AddConnection(clientIP) {
		_ = conn.Close()
		return
	}

	requestID := getRequestID(r)
	go h.handleConnection(conn, identity, clientIP, requestID)
}

func getRequestID(r *http.Request) string {
	if requestID, ok := ctxkey.GetRequestID(r.Context()); ok {
		return requestID
	}
	return r.Header.Get("X-Request-ID")
}

func (h *Handler) handleConnection(conn core.Conn, identity core.Identity, clientIP, requestID string) {
	userID := identity.UserID
	defer h.config.ConnectionLimiter.RemoveConnection(clientIP)
	defer func() { _ = conn.Close() }()
	defer panicrecovery.Catch(wsrecovery.Handler(wsrecovery.Metadata{
		Component: "adapter/nethttp.connection",
		UserID:    userID,
		ClientIP:  clientIP,
		RequestID: requestID,
	}))

	shardID := h.manager.GetShardID(userID)
	if err := h.manager.HandleIdentityConnection(conn, shardID, identity, clientIP, requestID); err != nil {
		logger.ErrorAsync("Failed to handle WebSocket connection in manager", "error", err, "userID", userID)
	}
}

// HandleStats returns manager, limiter, and pubsub runtime statistics.
func (h *Handler) HandleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"manager":     h.manager.GetStats(),
		"rateLimiter": h.config.ConnectionLimiter.GetStats(),
		"pubsub":      h.manager.GetPubSubManager().GetStats(),
		"timestamp":   time.Now(),
	})
}

// HandleShardManagement returns statistics for a requested shard.
func (h *Handler) HandleShardManagement(w http.ResponseWriter, r *http.Request) {
	shardID := r.URL.Query().Get("shard")
	if shardID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "shard ID required"})
		return
	}
	if shard, exists := h.manager.GetShard(shardID); exists {
		writeJSON(w, http.StatusOK, map[string]any{
			"shardID": shardID, "connections": shard.GetConnectionCount(), "exists": true,
		})
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": "shard not found"})
}

// HandleHealthCheck reports manager and cluster node health metadata.
func (h *Handler) HandleHealthCheck(w http.ResponseWriter, _ *http.Request) {
	readiness := h.manager.Readiness()
	status := http.StatusOK
	if !readiness.Ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"status": map[bool]string{true: "ready", false: "not_ready"}[readiness.Ready], "checks": readiness})
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

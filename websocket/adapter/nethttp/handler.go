// Package nethttp provides a WebSocket adapter for the standard library's
// net/http package.
package nethttp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/thanhbvha/go-common/v2/internal/httpkit"
	"github.com/thanhbvha/go-common/v2/logger"
	panicrecovery "github.com/thanhbvha/go-common/v2/recovery"
	"github.com/thanhbvha/go-common/v2/utils/ctxkey"
	"github.com/thanhbvha/go-common/v2/websocket/adapter/internal/wshttp"
	"github.com/thanhbvha/go-common/v2/websocket/core"
	"github.com/thanhbvha/go-common/v2/websocket/internal/wsrecovery"
	"github.com/thanhbvha/go-common/v2/websocket/limiter"
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
	// IPRateLimiter limits upgrade attempts per direct peer IP before the
	// request is authenticated. The default allows wshttp.DefaultIPRate/Burst.
	IPRateLimiter *limiter.KeyedRateLimiter
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
	gate    wshttp.Gate
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
	if config.ConnectionLimiter == nil {
		config.ConnectionLimiter = limiter.NewConnectionLimiter(100)
	}
	if config.ConnectionKey == nil {
		config.ConnectionKey = func(r *http.Request, _ string) string { return httpkit.ClientIP(r) }
	}

	if config.Manager == nil {
		config.Manager = core.NewManager()
	}
	return &Handler{config: config, manager: config.Manager, gate: wshttp.NewGate(config.Manager, config.IPRateLimiter)}
}

// HandleUpgrade authenticates, limits, and upgrades an HTTP request before
// registering the connection with the core manager.
func (h *Handler) HandleUpgrade(w http.ResponseWriter, r *http.Request) {
	if denial := h.gate.Admit(httpkit.ClientIP(r)); denial != nil {
		w.Header().Set("Retry-After", denial.RetryAfterHeader())
		writeJSON(w, denial.Status, map[string]string{"error": denial.Message})
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

	if denial := h.gate.AdmitIdentity(identity); denial != nil {
		w.Header().Set("Retry-After", denial.RetryAfterHeader())
		writeJSON(w, denial.Status, map[string]string{"error": denial.Message})
		return
	}

	clientIP := h.config.ConnectionKey(r, userID)
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

// HandleLive is the liveness probe: 200 whenever the process can answer. It
// never consults the manager or a broker, so a broker outage or a draining node
// does not get a healthy process restarted. Mount it at /livez.
func (h *Handler) HandleLive(w http.ResponseWriter, _ *http.Request) {
	status, body := wshttp.LiveStatus()
	writeJSON(w, status, body)
}

// HandleReady is the readiness probe: 200 only while this node should receive
// new connections, otherwise 503. Mount it at /readyz. It reveals internal state
// (broker and executor status), so serve it on an internal listener.
func (h *Handler) HandleReady(w http.ResponseWriter, _ *http.Request) {
	status, body := wshttp.ReadyStatus(h.manager.Readiness())
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

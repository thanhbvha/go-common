package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	bus "github.com/thanhbvha/go-common/v2/eventbus"
	"github.com/thanhbvha/go-common/v2/web/validator"
	adapter "github.com/thanhbvha/go-common/v2/websocket/adapter/nethttp"
	"github.com/thanhbvha/go-common/v2/websocket/core"
	bridge "github.com/thanhbvha/go-common/v2/websocket/integration/eventbus"
	"github.com/thanhbvha/go-common/v2/websocket/limiter"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
)

type roomPayload struct {
	RoomID string `json:"room_id" validate:"required,max=128"`
}
type textPayload struct {
	Text string `json:"text" validate:"required,max=512"`
}
type chatPayload struct {
	RoomID string `json:"room_id" validate:"required,max=128"`
	Text   string `json:"text" validate:"required,max=512"`
}
type notificationRequest struct {
	MessageID string `json:"message_id" validate:"omitempty,max=128"`
	TenantID  string `json:"tenant_id" validate:"required,max=64"`
	UserID    string `json:"user_id" validate:"omitempty,max=64"`
	SessionID string `json:"session_id" validate:"omitempty,max=128"`
	RoomID    string `json:"room_id" validate:"omitempty,max=128"`
	Text      string `json:"text" validate:"required,max=512"`
}

func decode[T any](raw []byte) (T, error) {
	var value T
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, core.ErrInvalidPayload
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return value, core.ErrInvalidPayload
	}
	violations, err := validator.Validate(value)
	if err != nil || len(violations) != 0 {
		return value, core.ErrInvalidPayload
	}
	return value, nil
}

func validatePayload[T any](raw []byte) error { _, err := decode[T](raw); return err }
func send(c *core.Connection, event string, data any) error {
	if !c.SendJSON(core.OutgoingMessage{Type: event, Data: data}) {
		return routing.ErrQueueFull
	}
	return nil
}

func (a *application) events() *core.EventRegistry {
	r := core.NewEventRegistry()
	local := func(event string, validate func([]byte) error, handler core.ContextJobHandler) {
		r.Register(event, core.HandlerConfig{Timeout: 3 * time.Second, MaxPayloadBytes: 2 << 10, Validate: validate, Handler: func(ctx context.Context, c *core.Connection, m core.IncomingMessage) error {
			if a.stopping.Load() {
				return routing.ErrUnavailable
			}
			if err := handler(ctx, c, m); err != nil {
				code := "operation_failed"
				if errors.Is(err, core.ErrUnauthorized) {
					code = "forbidden"
				}
				return errors.Join(err, send(c, "error", map[string]string{"code": code}))
			}
			return nil
		}})
	}
	local("session.info", nil, func(_ context.Context, c *core.Connection, _ core.IncomingMessage) error {
		i := c.Identity()
		return send(c, "session.info", map[string]any{"identity": map[string]any{"tenant_id": i.TenantID, "user_id": i.UserID, "account_id": i.AccountID, "session_id": i.SessionID, "expires_at": i.ExpiresAt}, "shard_id": c.GetShardID()})
	})
	local("system.ping", nil, func(_ context.Context, c *core.Connection, _ core.IncomingMessage) error {
		return send(c, "system.pong", map[string]string{"status": "ok"})
	})
	local("room.join", validatePayload[roomPayload], func(ctx context.Context, c *core.Connection, m core.IncomingMessage) error {
		p, err := decode[roomPayload](m.Data)
		if err != nil {
			return err
		}
		if err = c.GetShard().JoinRoom(ctx, p.RoomID, c); err != nil {
			return err
		}
		return send(c, "room.joined", p)
	})
	local("room.leave", validatePayload[roomPayload], func(ctx context.Context, c *core.Connection, m core.IncomingMessage) error {
		p, err := decode[roomPayload](m.Data)
		if err != nil {
			return err
		}
		if err = (policy{}).CanSubscribe(ctx, c.Identity(), p.RoomID); err != nil {
			return err
		}
		if err = c.GetShard().LeaveRoom(ctx, p.RoomID, c); err != nil {
			return err
		}
		return send(c, "room.left", p)
	})
	local("chat.send", validatePayload[chatPayload], func(ctx context.Context, c *core.Connection, m core.IncomingMessage) error {
		p, err := decode[chatPayload](m.Data)
		if err != nil {
			return err
		}
		if err = (policy{}).CanSubscribe(ctx, c.Identity(), p.RoomID); err != nil {
			return err
		}
		payload, err := json.Marshal(core.OutgoingMessage{Type: "chat.message", Data: map[string]string{"room_id": p.RoomID, "text": p.Text, "sender": c.Identity().UserID}})
		if err != nil {
			return err
		}
		result, err := a.router.SendToRoom(ctx, c.Identity().TenantID, p.RoomID, payload, routing.NewID())
		if err != nil {
			return err
		}
		return send(c, "chat.sent", result)
	})
	for _, target := range []string{"user", "session"} {
		local("notify."+target, validatePayload[textPayload], func(ctx context.Context, c *core.Connection, m core.IncomingMessage) error {
			p, err := decode[textPayload](m.Data)
			if err != nil {
				return err
			}
			payload, err := json.Marshal(core.OutgoingMessage{Type: "notification", Data: p})
			if err != nil {
				return err
			}
			i := c.Identity()
			var result routing.DeliveryResult
			if target == "session" {
				result, err = a.router.SendToSession(ctx, i.TenantID, i.UserID, i.SessionID, payload, routing.NewID())
			} else {
				result, err = a.router.SendToUser(ctx, i.TenantID, i.UserID, payload, routing.NewID())
			}
			if err != nil {
				return err
			}
			return send(c, "notify."+target+".sent", result)
		})
	}
	r.Register("business.echo", core.HandlerConfig{Dispatch: core.Business, Timeout: 3 * time.Second, MaxPayloadBytes: 2 << 10, Validate: validatePayload[textPayload]})
	return r
}

func (a *application) handleBusiness(ctx context.Context, j bus.Job) error {
	var command routing.BusinessCommand
	if err := j.Decode(&command); err != nil {
		return bus.MarkPermanent(core.ErrInvalidPayload)
	}
	if command.Type != "business.echo" || command.TenantID == "" || command.UserID == "" || command.SessionID == "" || command.MessageID == "" {
		return bus.MarkPermanent(core.ErrInvalidPayload)
	}
	p, err := decode[textPayload](command.Payload)
	if err != nil {
		return bus.MarkPermanent(err)
	}
	// Echo has no external side effect. Real business writes require application
	// idempotency and an outbox/inbox; gateway dedupe cannot provide DB atomicity.
	payload, err := json.Marshal(core.OutgoingMessage{Type: "business.completed", Data: p, RequestID: command.RequestID})
	if err != nil {
		return err
	}
	return a.bridge.PublishNotification(ctx, bridge.Notification{MessageID: command.MessageID + ":completed", TenantID: command.TenantID, UserID: command.UserID, SessionID: command.SessionID, Payload: payload})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func (a *application) handler() http.Handler {
	mux := http.NewServeMux()
	h := adapter.NewHandler(adapter.Config{Manager: a.manager, AuthenticateIdentity: a.authenticate, RateLimiter: limiter.NewRateLimiter(100, 200), ConnectionLimiter: limiter.NewConnectionLimiter(50)})
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		if a.stopping.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "draining"})
			return
		}
		h.HandleUpgrade(w, r)
	})
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if a.stopping.Load() || !a.manager.Readiness().Ready {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 250*time.Millisecond)
		defer cancel()
		if a.bus.Ping(ctx) != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.Handle("GET /admin/stats", a.admin(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		stats := a.manager.GetStats()
		delete(stats, "shardStats")
		writeJSON(w, http.StatusOK, map[string]any{"manager": stats, "gateway": a.gateway.Stats(), "router": a.router.Stats()})
	})))
	mux.Handle("POST /admin/notifications", a.admin(http.HandlerFunc(a.notify)))
	return mux
}

func (a *application) admin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !matches(r.Header.Get("Authorization"), a.settings.adminToken) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		if a.stopping.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "draining"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *application) notify(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4<<10)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "body_too_large"})
		return
	}
	n, err := decode[notificationRequest](raw)
	if err != nil || (n.RoomID != "" && (n.UserID != "" || n.SessionID != "")) || (n.RoomID == "" && n.UserID == "") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_target_or_payload"})
		return
	}
	if n.MessageID == "" {
		n.MessageID = routing.NewID()
	}
	payload, err := json.Marshal(core.OutgoingMessage{Type: "notification", Data: textPayload{Text: n.Text}})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "encode_failed"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	var result routing.DeliveryResult
	switch {
	case n.RoomID != "":
		result, err = a.router.SendToRoom(ctx, n.TenantID, n.RoomID, payload, n.MessageID)
	case n.SessionID != "":
		result, err = a.router.SendToSession(ctx, n.TenantID, n.UserID, n.SessionID, payload, n.MessageID)
	default:
		result, err = a.router.SendToUser(ctx, n.TenantID, n.UserID, payload, n.MessageID)
	}
	status := http.StatusOK
	if err != nil || result.Retryable() {
		status = http.StatusServiceUnavailable
	}
	// Partial/unknown results remain visible. Retry the same message ID; a 503
	// does not mean no recipient accepted the earlier attempt.
	writeJSON(w, status, map[string]any{"message_id": n.MessageID, "result": result})
}

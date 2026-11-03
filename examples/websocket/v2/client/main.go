// Command client exercises the v2 example without putting credentials in URLs.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/thanhbvha/go-common/v2/websocket/core"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
)

type options struct{ endpoint, mode, room, text, tenant, user, session, id string }
type client struct{ conn *websocket.Conn }

func main() {
	var o options
	flag.StringVar(&o.endpoint, "url", "ws://127.0.0.1:8090/ws", "WebSocket URL (no credentials)")
	flag.StringVar(&o.mode, "mode", "script", "script, listen or notify")
	flag.StringVar(&o.room, "room", "demo:lobby", "Room to join; notify uses this instead of -user when -user is empty")
	flag.StringVar(&o.text, "text", "hello from v2", "Message text")
	flag.StringVar(&o.tenant, "tenant", "demo", "Admin notification tenant")
	flag.StringVar(&o.user, "user", "alice", "Admin notification user; use -user='' for a room")
	flag.StringVar(&o.session, "session", "", "Admin notification session; omitted sends to every user session")
	flag.StringVar(&o.id, "message-id", "", "Stable notification ID to reuse on retry")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, o); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func endpoint(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("use a ws/wss URL without credentials, query parameters or fragments")
	}
	return u, nil
}

func run(ctx context.Context, o options) error {
	u, err := endpoint(o.endpoint)
	if err != nil {
		return err
	}
	if o.mode == "notify" {
		return notify(ctx, u, o)
	}
	if o.mode != "script" && o.mode != "listen" {
		return errors.New("mode must be script, listen or notify")
	}
	token := os.Getenv("WS_V2_TOKEN")
	if token == "" {
		return errors.New("WS_V2_TOKEN is required")
	}
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, response, err := dialer.DialContext(ctx, u.String(), http.Header{"Authorization": []string{"Bearer " + token}})
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return errors.New("WebSocket handshake failed; check credential, origin and readiness")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	conn.SetReadLimit(32 << 10)
	c := client{conn: conn}
	if err = c.request("session.info", map[string]any{}, "session.info"); err != nil {
		return err
	}
	if err = c.request("room.join", map[string]string{"room_id": o.room}, "room.joined"); err != nil {
		return err
	}
	if o.mode == "listen" {
		_ = conn.SetReadDeadline(time.Time{})
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return errors.New("WebSocket listener disconnected")
			}
			fmt.Println(string(data))
		}
	}
	for _, step := range []struct {
		event, want string
		data        any
	}{
		{"system.ping", "system.pong", map[string]any{}},
		{"chat.send", "chat.sent", map[string]string{"room_id": o.room, "text": o.text}},
		{"notify.user", "notify.user.sent", map[string]string{"text": o.text}},
		{"notify.session", "notify.session.sent", map[string]string{"text": o.text}},
		{"business.echo", "business.completed", map[string]string{"text": o.text}},
		{"room.leave", "room.left", map[string]string{"room_id": o.room}},
	} {
		if err = c.request(step.event, step.data, step.want); err != nil {
			return err
		}
	}
	return conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "script complete"), time.Now().Add(time.Second))
}

func (c client) request(event string, data any, want string) error {
	deadline := time.Now().Add(8 * time.Second)
	payload, err := json.Marshal(data)
	if err != nil {
		return errors.New("cannot encode event payload")
	}
	_ = c.conn.SetWriteDeadline(deadline)
	if err := c.conn.WriteJSON(core.IncomingMessage{Type: event, Data: payload}); err != nil {
		return errors.New("cannot send WebSocket event")
	}
	_ = c.conn.SetReadDeadline(deadline)
	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			return errors.New("timed out or disconnected waiting for event result")
		}
		var reply struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if json.Unmarshal(raw, &reply) != nil {
			return errors.New("invalid server frame")
		}
		fmt.Println(string(raw))
		if reply.Type == "error" {
			return errors.New("server rejected the operation")
		}
		if reply.Type == want {
			if want == "chat.sent" || want == "notify.user.sent" || want == "notify.session.sent" {
				var result routing.DeliveryResult
				if json.Unmarshal(reply.Data, &result) != nil || result.Retryable() || result.Offline || result.Count(routing.Enqueued) == 0 {
					return errors.New("routing did not accept all expected recipients")
				}
			}
			return nil
		}
	}
}

func notify(ctx context.Context, u *url.URL, o options) error {
	token := os.Getenv("WS_V2_ADMIN_TOKEN")
	if token == "" {
		return errors.New("WS_V2_ADMIN_TOKEN is required")
	}
	id := o.id
	if id == "" {
		id = routing.NewID()
	}
	room := o.room
	if o.user != "" {
		room = ""
	}
	body, err := json.Marshal(map[string]string{"message_id": id, "tenant_id": o.tenant, "user_id": o.user, "session_id": o.session, "room_id": room, "text": o.text})
	if err != nil {
		return err
	}
	u.Path = "/admin/notifications"
	if u.Scheme == "wss" {
		u.Scheme = "https"
	} else {
		u.Scheme = "http"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return errors.New("cannot construct admin request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return errors.New("admin request failed")
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil {
		return errors.New("cannot read admin response")
	}
	fmt.Println(string(data))
	if response.StatusCode != http.StatusOK {
		return errors.New("delivery was rejected or incomplete; inspect the result and retry the same message ID when appropriate")
	}
	return nil
}

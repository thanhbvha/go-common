package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/thanhbvha/go-common/v2/websocket/core"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
)

// Synthetic, predictable fixtures; no real credentials are stored here.
func fixtureCredentials() []credential {
	var result []credential
	for _, p := range []struct{ marker, tenant, user, session string }{
		{"a", "demo", "alice", "desktop"}, {"p", "demo", "alice", "phone"},
		{"b", "demo", "bob", "desktop"}, {"o", "other", "alice", "desktop"},
	} {
		result = append(result, credential{strings.Repeat(p.marker, 32), core.Identity{TenantID: p.tenant, UserID: p.user, SessionID: p.session, AccountID: p.tenant + "-members", ExpiresAt: time.Now().Add(time.Hour)}})
	}
	return result
}

func exampleSettings(t *testing.T, delivery, business string) settings {
	t.Helper()
	s := defaults()
	s.credentials = fixtureCredentials()
	s.adminToken = strings.Repeat("z", 32)
	s.business = business
	s.delivery = delivery
	s.namespace = "ws-v2-test-" + routing.NewID()
	s.drainTimeout = time.Second
	s.roomNodeIndex = true // This isolated namespace has only upgraded writers.
	redis := miniredis.RunT(t)
	s.redisURL = "redis://" + redis.Addr() + "/0"
	if delivery == "nats" {
		nats, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: server.RANDOM_PORT, NoLog: true, NoSigs: true})
		if err != nil {
			t.Fatal("NATS startup configuration")
		}
		go nats.Start()
		if !nats.ReadyForConnections(time.Second) {
			t.Fatal("NATS startup timeout")
		}
		t.Cleanup(func() { nats.Shutdown(); nats.WaitForShutdown() })
		s.natsURL = nats.ClientURL()
	}
	return s
}

func startExample(t *testing.T, s settings) (*application, *httptest.Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a, err := newApplication(ctx, s)
	if err != nil {
		t.Fatalf("example startup: %v", err)
	}
	httpServer := httptest.NewServer(a.handler())
	t.Cleanup(func() {
		httpServer.Close()
		if err := a.close(); err != nil {
			t.Errorf("example shutdown: %v", err)
		}
	})
	return a, httpServer
}

func dialExample(t *testing.T, s *httptest.Server, c credential) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(s.URL, "http") + "/ws?tenant_id=forged&user_id=forged&session_id=forged"
	conn, response, err := websocket.DefaultDialer.Dial(url, http.Header{"Authorization": []string{"Bearer " + c.token}})
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		t.Fatal("authenticated WebSocket dial failed")
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

type frame struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func readEvent(t *testing.T, c *websocket.Conn, want string) frame {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var value frame
	if err := c.ReadJSON(&value); err != nil {
		t.Fatalf("waiting for %s: %v", want, err)
	}
	if value.Type != want {
		t.Fatalf("event=%s want=%s", value.Type, want)
	}
	return value
}
func sendEvent(t *testing.T, c *websocket.Conn, event string, data any) {
	t.Helper()
	_ = c.SetWriteDeadline(time.Now().Add(time.Second))
	if err := c.WriteJSON(map[string]any{"type": event, "data": data}); err != nil {
		t.Fatalf("send %s: %v", event, err)
	}
}
func request(t *testing.T, c *websocket.Conn, event, want string, data any) frame {
	t.Helper()
	sendEvent(t, c, event, data)
	return readEvent(t, c, want)
}
func postNotification(t *testing.T, s *httptest.Server, token string, n notificationRequest) (int, routing.DeliveryResult) {
	t.Helper()
	body, err := json.Marshal(n)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, s.URL+"/admin/notifications", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := s.Client().Do(req)
	if err != nil {
		t.Fatal("notification HTTP request failed")
	}
	defer response.Body.Close()
	var decoded struct {
		Result routing.DeliveryResult `json:"result"`
	}
	if err = json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatal("invalid notification response")
	}
	return response.StatusCode, decoded.Result
}

func TestV2EndToEndAcrossGateways(t *testing.T) {
	for _, combination := range []struct{ delivery, business string }{{"redis", "memory"}, {"redis", "redis"}, {"nats", "redis"}} {
		t.Run(combination.delivery+"-"+combination.business, func(t *testing.T) {
			s := exampleSettings(t, combination.delivery, combination.business)
			a, one := startExample(t, s)
			b, two := startExample(t, s)
			alice := dialExample(t, one, s.credentials[0])
			phone := dialExample(t, two, s.credentials[1])
			bob := dialExample(t, two, s.credentials[2])
			foreign := dialExample(t, two, s.credentials[3])
			info := request(t, alice, "session.info", "session.info", map[string]any{})
			var body struct {
				Identity struct {
					Tenant  string `json:"tenant_id"`
					User    string `json:"user_id"`
					Session string `json:"session_id"`
				} `json:"identity"`
			}
			if json.Unmarshal(info.Data, &body) != nil || body.Identity.Tenant != "demo" || body.Identity.User != "alice" || body.Identity.Session != "desktop" {
				t.Fatal("query parameters changed authenticated identity")
			}
			for _, c := range []*websocket.Conn{alice, phone, bob} {
				request(t, c, "room.join", "room.joined", roomPayload{"demo:lobby"})
			}
			request(t, foreign, "room.join", "error", roomPayload{"demo:lobby"})
			request(t, foreign, "room.join", "room.joined", roomPayload{"other:lobby"})
			sendEvent(t, alice, "chat.send", chatPayload{"demo:lobby", "room test"})
			readEvent(t, alice, "chat.message")
			chat := readEvent(t, alice, "chat.sent")
			var result routing.DeliveryResult
			if json.Unmarshal(chat.Data, &result) != nil || result.Count(routing.Enqueued) != 3 {
				t.Fatal("cross-node room delivery failed")
			}
			readEvent(t, phone, "chat.message")
			readEvent(t, bob, "chat.message")
			sendEvent(t, alice, "notify.user", textPayload{"all my devices"})
			readEvent(t, alice, "notification")
			readEvent(t, alice, "notify.user.sent")
			readEvent(t, phone, "notification")
			sendEvent(t, alice, "notify.session", textPayload{"only this session"})
			readEvent(t, alice, "notification")
			readEvent(t, alice, "notify.session.sent")
			n := notificationRequest{MessageID: "stable-notification", TenantID: "demo", UserID: "alice", Text: "admin notification"}
			status, result := postNotification(t, one, s.adminToken, n)
			if status != http.StatusOK || result.Count(routing.Enqueued) != 2 {
				t.Fatalf("user result: status=%d result=%+v", status, result)
			}
			readEvent(t, alice, "notification")
			readEvent(t, phone, "notification")
			before := a.gateway.Stats().Enqueued + b.gateway.Stats().Enqueued
			status, result = postNotification(t, one, s.adminToken, n)
			if status != http.StatusOK || result.Count(routing.Enqueued) != 2 || a.gateway.Stats().Enqueued+b.gateway.Stats().Enqueued != before {
				t.Fatal("same notification ID enqueued twice")
			}
			completed := request(t, alice, "business.echo", "business.completed", textPayload{"through EventBus"})
			var echoed textPayload
			if json.Unmarshal(completed.Data, &echoed) != nil || echoed.Text != "through EventBus" {
				t.Fatal("business command did not return through routing")
			}
			request(t, alice, "room.leave", "room.left", roomPayload{"demo:lobby"})
			status, result = postNotification(t, one, s.adminToken, notificationRequest{TenantID: "demo", RoomID: "demo:lobby", Text: "remaining room members"})
			if status != http.StatusOK || result.Count(routing.Enqueued) != 2 {
				t.Fatal("room leave did not update routing")
			}
			readEvent(t, phone, "notification")
			readEvent(t, bob, "notification")
		})
	}
}

func TestV2HTTPBoundariesAndShutdown(t *testing.T) {
	s := exampleSettings(t, "redis", "memory")
	a, httpServer := startExample(t, s)
	conn := dialExample(t, httpServer, s.credentials[0])
	request(t, conn, "system.ping", "system.pong", map[string]any{})
	for _, path := range []string{"/admin/stats", "/admin/notifications"} {
		method := http.MethodGet
		if path == "/admin/notifications" {
			method = http.MethodPost
		}
		req, _ := http.NewRequest(method, httpServer.URL+path, strings.NewReader("{}"))
		req.Header.Set("Authorization", "Bearer "+s.credentials[0].token)
		response, err := httpServer.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatal("client credential authorized admin endpoint")
		}
	}
	status, _ := postNotification(t, httpServer, s.adminToken, notificationRequest{TenantID: "demo", UserID: "alice", RoomID: "demo:lobby", Text: "invalid selector"})
	if status != http.StatusBadRequest {
		t.Fatal("ambiguous target accepted")
	}
	req, _ := http.NewRequest(http.MethodPost, httpServer.URL+"/admin/notifications", strings.NewReader(strings.Repeat("x", 5000)))
	req.Header.Set("Authorization", "Bearer "+s.adminToken)
	response, err := httpServer.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatal("HTTP body limit not enforced")
	}
	a.stopping.Store(true)
	response, err = httpServer.Client().Get(httpServer.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("shutdown remained ready")
	}
	if err = a.close(); err != nil {
		t.Fatal(err)
	}
	if a.manager.State() != core.StateStopped || a.gateway.Stats().Sessions != 0 {
		t.Fatal("shutdown left active connections or registry entries")
	}
	metrics := a.manager.GetStats()["runtime"].(core.RuntimeMetrics)
	if metrics.ConnectionsDrained == 0 || metrics.DrainForced != 0 {
		t.Fatalf("shutdown metrics: %+v", metrics)
	}
	if err = a.close(); err != nil {
		t.Fatal("repeat shutdown changed result")
	}
}

func TestV2RejectsInvalidCredentialConfiguration(t *testing.T) {
	s := defaults()
	s.credentials = fixtureCredentials()
	s.adminToken = s.credentials[0].token
	if s.validate() == nil {
		t.Fatal("admin credential may equal a client credential")
	}
	s.adminToken = strings.Repeat("z", 32)
	s.credentials[0].token = "short"
	if s.validate() == nil {
		t.Fatal("weak credential accepted")
	}
}

func TestV2ScriptClientAgainstRunningExample(t *testing.T) {
	s := exampleSettings(t, "redis", "memory")
	_, server := startExample(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "run", "./examples/websocket/v2/client", "-mode=script", "-url=ws"+strings.TrimPrefix(server.URL, "http")+"/ws")
	command.Dir = "../../.." // Execute the documented CLI command from repository root.
	command.Env = append(os.Environ(), "WS_V2_TOKEN="+s.credentials[0].token)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("script client did not complete: %v", err)
	}
	if strings.Contains(string(output), s.credentials[0].token) {
		t.Fatal("client printed a credential")
	}
	if !strings.Contains(string(output), "business.completed") || !strings.Contains(string(output), "room.left") {
		t.Fatal("script omitted a required protocol step")
	}
}

func TestV2RejectsClientSelectedIdentityFields(t *testing.T) {
	s := exampleSettings(t, "redis", "memory")
	a, server := startExample(t, s)
	conn := dialExample(t, server, s.credentials[0])
	sendEvent(t, conn, "notify.user", map[string]string{"text": "forged target", "tenant_id": "other", "user_id": "bob"})
	request(t, conn, "system.ping", "system.pong", map[string]any{})
	stats := a.manager.GetStats()["events"].(core.EventStats)
	if stats.Malformed != 1 || a.gateway.Stats().Enqueued != 0 {
		t.Fatal("client-selected target bypassed strict payload validation")
	}
}

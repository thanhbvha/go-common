package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func testOptions(target string) options {
	return options{target: target, token: "test", tenant: "demo", account: "member", event: "system.ping", scenario: "message", connections: 1, messages: 2, reconnects: 3, groupSize: 5, accounts: 1}
}

type peer struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (p *peer) write(kind string, data any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = p.conn.SetWriteDeadline(time.Now().Add(time.Second))
	_ = p.conn.WriteJSON(map[string]any{"type": kind, "data": data})
}

func TestSocketWorkloads(t *testing.T) {
	var mu sync.Mutex
	rooms := map[string][]*peer{}
	sessions := []string{}
	users := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		mu.Lock()
		sessions = append(sessions, r.URL.Query().Get("session_id"))
		users = append(users, r.URL.Query().Get("user_id"))
		mu.Unlock()
		p := &peer{conn: conn}
		p.write("session.info", map[string]any{"ready": true})
		for {
			var m struct {
				Type string `json:"type"`
				Data struct {
					Room string `json:"room_id"`
					Text string `json:"text"`
				} `json:"data"`
			}
			if conn.ReadJSON(&m) != nil {
				return
			}
			switch m.Type {
			case "system.ping":
				p.write("system.pong", nil)
			case "room.join":
				mu.Lock()
				rooms[m.Data.Room] = append(rooms[m.Data.Room], p)
				mu.Unlock()
				p.write("room.joined", nil)
			case "chat.send":
				mu.Lock()
				members := append([]*peer(nil), rooms[m.Data.Room]...)
				mu.Unlock()
				for _, member := range members {
					member.write("chat.message", m.Data.Text)
				}
			}
		}
	}))
	defer server.Close()
	target := "ws" + strings.TrimPrefix(server.URL, "http")
	for _, scenario := range []string{"message", "reconnect", "churn"} {
		t.Run(scenario, func(t *testing.T) {
			mu.Lock()
			sessions = nil
			users = nil
			mu.Unlock()
			o := testOptions(target)
			o.scenario = scenario
			got := run(o)
			if got.ConnectErrors != 0 || got.MessageErrors != 0 || got.Missing != 0 || got.Active != 0 || got.Connected != int64(o.cycles()) {
				t.Fatalf("result: %+v", got)
			}
			mu.Lock()
			defer mu.Unlock()
			for i := 1; i < len(sessions); i++ {
				if users[i] != users[0] {
					t.Fatal("user changed across cycles")
				}
				if (sessions[i] == sessions[0]) != (scenario == "reconnect") {
					t.Fatalf("wrong session reuse for %s", scenario)
				}
			}
		})
	}
	for _, size := range []int{5, 10} {
		o := testOptions(target)
		o.scenario = "fanout"
		o.groupSize = size
		o.connections = size * 2
		o.messages = 3
		if err := o.validate(); err != nil {
			t.Fatal(err)
		}
		got := run(o)
		if got.ConnectErrors != 0 || got.MessageErrors != 0 || got.Missing != 0 || got.Duplicates != 0 || got.Received != int64(o.connections*o.messages*size) || got.Sent != int64(o.connections*o.messages) || got.PeakActive != int64(o.connections) || got.Active != 0 {
			t.Fatalf("fanout size %d: %+v", size, got)
		}
	}
}

func TestIdleProcessesPingPong(t *testing.T) {
	var mu sync.Mutex
	pongs := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
		conn.SetPongHandler(func(string) error {
			mu.Lock()
			pongs++
			mu.Unlock()
			return conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
		})
		done := make(chan struct{})
		defer close(done)
		go func() {
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					_ = conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second))
				case <-done:
					return
				}
			}
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	o := testOptions("ws" + strings.TrimPrefix(server.URL, "http"))
	o.scenario = "idle"
	o.hold = 350 * time.Millisecond
	got := run(o)
	mu.Lock()
	defer mu.Unlock()
	if got.MessageErrors != 0 || got.Active != 0 || pongs < 3 {
		t.Fatalf("pongs=%d result=%+v", pongs, got)
	}
}

func TestWorkloadValidation(t *testing.T) {
	base := testOptions("ws://127.0.0.1:8080/ws")
	for _, change := range []func(*options){func(o *options) { o.connections = -1 }, func(o *options) { o.scenario = "unknown" }, func(o *options) { o.scenario = "fanout"; o.connections = 7 }, func(o *options) { o.slowReaders = 101 }, func(o *options) { o.target = "ws://user:secret@localhost/ws" }, func(o *options) { o.connections = 100000; o.messages = 100000 }} {
		o := base
		change(&o)
		if o.validate() == nil {
			t.Fatal("accepted invalid options")
		}
	}
	// Keep JSON output names explicit about which process owns runtime statistics.
	encoded, _ := json.Marshal(result{})
	if !strings.Contains(string(encoded), "clientHeapAfterBytes") {
		t.Fatal("ambiguous heap metric")
	}
}

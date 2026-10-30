//go:build integration

package main

import (
	"context"
	"github.com/google/uuid"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats-server/v2/server"
	goredis "github.com/redis/go-redis/v9"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}
func TestClusterExampleEndToEnd(t *testing.T) {
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not installed")
	}
	t.Setenv("WS_EXAMPLE_CLIENT_TOKEN", "integration-token")
	for _, tc := range []struct{ name, eventbus, delivery, legacy string }{
		{name: "legacy-redis", legacy: "redis"}, {name: "legacy-nats", legacy: "nats"},
		{name: "redis-nats", eventbus: "redis", delivery: "nats"}, {name: "nats-redis", eventbus: "nats", delivery: "redis"},
		{name: "kafka-redis", eventbus: "kafka", delivery: "redis"}, {name: "kafka-nats", eventbus: "kafka", delivery: "nats"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.eventbus == "kafka" && os.Getenv("WS_KAFKA_INTEGRATION") != "1" {
				t.Skip("set WS_KAFKA_INTEGRATION=1 and WS_KAFKA_BROKERS to test Kafka")
			}
			if tc.eventbus == "kafka" && os.Getenv("WS_KAFKA_BROKERS") == "" {
				t.Fatal("WS_KAFKA_BROKERS is required when Kafka integration is enabled")
			}
			namespace := "ws_example_test_" + uuid.NewString()
			base := options{broker: tc.legacy, eventbusDriver: tc.eventbus, deliveryDriver: tc.delivery, kafkaBrokers: os.Getenv("WS_KAFKA_BROKERS"), namespace: namespace}
			normalized, err := base.normalized()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			redisAddr := freeAddress(t)
			_, redisPort, _ := net.SplitHostPort(redisAddr)
			redis := exec.Command(binary, "--bind", "127.0.0.1", "--port", redisPort, "--save", "", "--appendonly", "no")
			redis.Stdout = io.Discard
			redis.Stderr = io.Discard
			if err := redis.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { redis.Process.Kill(); redis.Wait() }()
			client := goredis.NewClient(&goredis.Options{Addr: redisAddr})
			defer client.Close()
			for client.Ping(ctx).Err() != nil {
				if ctx.Err() != nil {
					t.Fatal("Redis startup")
				}
				time.Sleep(10 * time.Millisecond)
			}
			natsURL := ""
			if normalized.eventbusDriver == "nats" || normalized.deliveryDriver == "nats" {
				s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: server.RANDOM_PORT, JetStream: normalized.eventbusDriver == "nats", StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
				if err != nil {
					t.Fatal(err)
				}
				go s.Start()
				if !s.ReadyForConnections(5 * time.Second) {
					t.Fatal("NATS startup")
				}
				defer func() { s.Shutdown(); s.WaitForShutdown() }()
				natsURL = s.ClientURL()
			}
			jobs := make(chan error, 4)
			addresses := []string{freeAddress(t), freeAddress(t)}
			for i, role := range []string{"worker", "router", "gateway", "gateway"} {
				o := base
				o.role = role
				o.redis = redisAddr
				o.nats = natsURL
				if role == "gateway" {
					o.addr = addresses[i-2]
				}
				go func() {
					err := run(ctx, o)
					jobs <- err
					if err != nil {
						cancel()
					}
				}()
			}
			// Ensure services stop before broker cleanup, including on assertion failure.
			defer func() {
				cancel()
				for i := 0; i < 4; i++ {
					select {
					case err := <-jobs:
						if err != nil {
							t.Error(err)
						}
					case <-time.After(7 * time.Second):
						t.Error("service shutdown timeout")
					}
				}
			}()
			clients := make([]*websocket.Conn, 0, 2)
			for i, addr := range addresses {
				var c *websocket.Conn
				for {
					c, _, err = websocket.DefaultDialer.Dial("ws://"+addr+"/ws?tenant_id=demo&user_id="+string(rune('a'+i))+"&session_id=device", http.Header{"Authorization": []string{"Bearer integration-token"}})
					if err == nil {
						break
					}
					if ctx.Err() != nil {
						t.Fatal(err)
					}
					time.Sleep(20 * time.Millisecond)
				}
				defer c.Close()
				clients = append(clients, c)
				c.SetReadDeadline(time.Now().Add(45 * time.Second))
				if err = c.WriteJSON(map[string]any{"type": "room.join", "data": map[string]any{"room_id": "demo:room"}}); err != nil {
					t.Fatal(err)
				}
				expectType(t, c, "room.joined")
			}
			if err = clients[0].WriteJSON(map[string]any{"type": "chat.send", "data": map[string]any{"room_id": "demo:room", "text": "hello"}}); err != nil {
				t.Fatal(err)
			}
			for _, c := range clients {
				expectType(t, c, "chat.message")
			}
			if err = clients[0].WriteJSON(map[string]any{"type": "business", "data": map[string]any{"answer": 42}}); err != nil {
				t.Fatal(err)
			}
			expectType(t, clients[0], "business.completed")
			// Drain a measured notification stream while sampling gateway enqueue latency.
			drained := make(chan error, 1)
			go func() {

				for i := 0; i < 100; i++ {
					if _, _, err := clients[0].ReadMessage(); err != nil {
						drained <- err
						return
					}
				}
				drained <- nil
			}()
			o := base
			o.role = "load"
			o.redis = redisAddr
			o.nats = natsURL
			o.tenant = "demo"
			o.user = "a"
			o.count = 100
			if err = run(ctx, o); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-drained:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("load frames missing")
			}
		})
	}
}
func expectType(t *testing.T, c *websocket.Conn, want string) {
	t.Helper()
	var message struct {
		Type string `json:"type"`
	}
	if err := c.ReadJSON(&message); err != nil {
		t.Fatal(err)
	}
	if message.Type != want {
		t.Fatalf("frame type=%s want=%s", message.Type, want)
	}
}

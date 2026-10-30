// The cluster example separates gateways, business workers and realtime routers.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	gonats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
	bus "github.com/thanhbvha/go-common/eventbus"
	natsbus "github.com/thanhbvha/go-common/eventbus/driver/nats"
	redisbus "github.com/thanhbvha/go-common/eventbus/driver/redis"
	"github.com/thanhbvha/go-common/eventbus/transport"
	adapter "github.com/thanhbvha/go-common/websocket/adapter/nethttp"
	"github.com/thanhbvha/go-common/websocket/core"
	bridge "github.com/thanhbvha/go-common/websocket/integration/eventbus"
	"github.com/thanhbvha/go-common/websocket/routing"
	natsrouting "github.com/thanhbvha/go-common/websocket/routing/nats"
	redisrouting "github.com/thanhbvha/go-common/websocket/routing/redis"
)

type options struct {
	role, broker, redis, nats, addr, tenant, user, room, text string
	count                                                     int
}

func main() {
	var o options
	flag.StringVar(&o.role, "role", "gateway", "gateway, worker, router, publish or load")
	flag.StringVar(&o.broker, "broker", "redis", "redis or nats")
	flag.StringVar(&o.redis, "redis", "127.0.0.1:6379", "Redis registry address (required for both stacks)")
	flag.StringVar(&o.nats, "nats", gonats.DefaultURL, "NATS URL; JetStream must be enabled")
	flag.StringVar(&o.addr, "addr", "127.0.0.1:8080", "gateway HTTP address")
	flag.StringVar(&o.tenant, "tenant", "demo", "tenant for publish/load")
	flag.StringVar(&o.user, "user", "alice", "user for publish/load")
	flag.StringVar(&o.room, "room", "", "room target instead of user")
	flag.StringVar(&o.text, "text", "hello", "notification text")
	flag.IntVar(&o.count, "count", 1000, "load sample count")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, o); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
func run(ctx context.Context, o options) error {
	switch o.role {
	case "gateway", "worker", "router", "publish", "load":
	default:
		return fmt.Errorf("unknown role %q", o.role)
	}
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rc := goredis.NewClient(&goredis.Options{Addr: o.redis})
	defer rc.Close()
	if err := rc.Ping(startup).Err(); err != nil {
		return err
	}
	registry := redisrouting.NewRegistry(rc, "ws-example")
	var delivery routing.DeliveryTransport
	var transportDriver transport.Transport
	var eventOptions []bus.EventOption
	switch o.broker {
	case "redis":
		delivery = redisrouting.NewTransport(rc, "ws-example")
		transportDriver = redisbus.New(rc)
		eventOptions = []bus.EventOption{redisbus.WithEventLayoutTargeted(), redisbus.WithBatchSize(64), redisbus.WithBlock(25 * time.Millisecond)}
	case "nats":
		nc, err := gonats.Connect(o.nats)
		if err != nil {
			return err
		}
		defer nc.Close()
		js, err := nc.JetStream()
		if err != nil {
			return err
		}
		delivery = natsrouting.NewTransport(nc, "ws-example")
		transportDriver = natsbus.New(js)
		eventOptions = []bus.EventOption{natsbus.WithEventLayoutTargeted(), natsbus.WithBatchSize(64), natsbus.WithFetchTimeout(25 * time.Millisecond)}
	default:
		return errors.New("broker must be redis or nats")
	}
	b, err := bus.Open(bus.DefaultConfig(), bus.WithTransport(transportDriver))
	if err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Drain(shutdown)
	}()
	var gateway *routing.Gateway
	if o.role == "gateway" {
		gateway, err = routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: delivery})
		if err != nil {
			return err
		}
		if err = gateway.Start(startup); err != nil {
			return err
		}
		defer func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = gateway.Close(shutdown)
		}()
	} else {
		if err = delivery.Start(startup, routing.NewID(), nil); err != nil {
			return err
		}
		defer delivery.Close(context.Background())
	}
	router, err := routing.NewRouter(routing.RouterConfig{Registry: registry, Transport: delivery})
	if err != nil {
		return err
	}
	config := bridge.Config{Bus: b, NotificationOptions: eventOptions}
	if o.role == "router" {
		config.Router = router
	}
	integration, err := bridge.New(config)
	if err != nil {
		return err
	}
	if o.role == "worker" {
		if err = b.HandleTask("ws.command", func(ctx context.Context, j bus.Job) error {
			var command routing.BusinessCommand
			if err := j.Decode(&command); err != nil {
				return bus.MarkPermanent(err)
			}
			// Replace with business logic and an application outbox when DB atomicity is required.
			payload, _ := json.Marshal(core.OutgoingMessage{Type: "business.completed", Data: json.RawMessage(command.Payload), RequestID: command.RequestID})
			return integration.PublishNotification(ctx, bridge.Notification{MessageID: j.ID() + "-completed", TenantID: command.TenantID, UserID: command.UserID, Payload: payload})
		}); err != nil {
			return err
		}
	}
	if err = b.Start(startup); err != nil {
		return err
	}
	switch o.role {
	case "publish":
		payload, _ := json.Marshal(core.OutgoingMessage{Type: "notification", Data: o.text})
		n := bridge.Notification{TenantID: o.tenant, UserID: o.user, RoomID: o.room, Payload: payload}
		if o.room != "" {
			n.UserID = ""
		}
		return integration.PublishNotification(ctx, n)
	case "load":
		return load(ctx, router, o)
	case "gateway":
		return serve(ctx, gateway, integration, router, o)
	default:
		log.Printf("%s ready (%s)", o.role, o.broker)
		<-ctx.Done()
		return nil
	}
}
func serve(ctx context.Context, gateway *routing.Gateway, publisher routing.CommandPublisher, router *routing.Router, o options) error {
	token := os.Getenv("WS_EXAMPLE_CLIENT_TOKEN")
	if token == "" {
		return errors.New("WS_EXAMPLE_CLIENT_TOKEN is required")
	}
	registry := core.NewEventRegistry()
	registry.Register("business", core.HandlerConfig{Dispatch: core.Business})
	registry.Register("room.join", core.HandlerConfig{Handler: func(ctx context.Context, c *core.Connection, m core.IncomingMessage) error {
		var p struct {
			RoomID string `json:"room_id"`
		}
		if err := json.Unmarshal(m.Data, &p); err != nil {
			return err
		}
		if err := c.GetShard().JoinRoom(ctx, p.RoomID, c); err != nil {
			return err
		}
		c.SendJSON(core.OutgoingMessage{Type: "room.joined", Data: p})
		return nil
	}})
	registry.Register("chat.send", core.HandlerConfig{Handler: func(ctx context.Context, c *core.Connection, m core.IncomingMessage) error {
		var p struct {
			RoomID string `json:"room_id"`
			Text   string `json:"text"`
		}
		if err := json.Unmarshal(m.Data, &p); err != nil {
			return err
		}
		if err := (roomAuthorizer{}).CanSubscribe(ctx, c.Identity(), p.RoomID); err != nil {
			return err
		}
		payload, _ := json.Marshal(core.OutgoingMessage{Type: "chat.message", Data: p.Text})
		res, err := router.SendToRoom(ctx, c.Identity().TenantID, p.RoomID, payload, "")
		if err == nil && res.Retryable() {
			err = routing.ErrUnavailable
		}
		return err
	}})
	cfg := core.DefaultManagerConfig()
	cfg.Distributed = gateway
	cfg.EventRegistry = registry
	cfg.CommandPublisher = publisher
	cfg.Authorizer = roomAuthorizer{}
	manager := core.NewManagerWithConfig(cfg)
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = manager.Shutdown(shutdown)
	}()
	h := adapter.NewHandler(adapter.Config{Manager: manager, AuthenticateIdentity: func(r *http.Request) (core.Identity, error) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			return core.Identity{}, core.ErrUnauthorized
		}
		q := r.URL.Query()
		identity := core.Identity{TenantID: q.Get("tenant_id"), UserID: q.Get("user_id"), SessionID: q.Get("session_id")}
		if identity.TenantID == "" || identity.UserID == "" {
			return identity, core.ErrUnauthorized
		}
		return identity, nil
	}})
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.HandleUpgrade)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if !manager.Readiness().Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	server := &http.Server{Addr: o.addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	log.Printf("gateway %s listening %s", gateway.NodeID(), o.addr)
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}

type roomAuthorizer struct{}

func (roomAuthorizer) CanSubscribe(_ context.Context, i core.Identity, room string) error {
	if !strings.HasPrefix(room, i.TenantID+":") {
		return core.ErrUnauthorized
	}
	return nil
}
func (roomAuthorizer) CanPublish(_ context.Context, i core.Identity, event string) error {
	if i.UserID == "" || i.TenantID == "" {
		return core.ErrUnauthorized
	}
	return nil
}
func load(ctx context.Context, router *routing.Router, o options) error {
	if o.count <= 0 {
		return errors.New("count must be positive")
	}
	samples := make([]time.Duration, 0, o.count)
	payload := []byte(`{"type":"load"}`)
	start := time.Now()
	for i := 0; i < o.count; i++ {
		before := time.Now()
		var res routing.DeliveryResult
		var err error
		if o.room != "" {
			res, err = router.SendToRoom(ctx, o.tenant, o.room, payload, "")
		} else {
			res, err = router.SendToUser(ctx, o.tenant, o.user, payload, "")
		}
		if err != nil {
			return err
		}
		if res.Offline || res.Retryable() {
			return fmt.Errorf("load needs connected healthy recipients: %+v", res)
		}
		samples = append(samples, time.Since(before))
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	fmt.Printf("samples=%d p95=%s p99=%s messages/sec=%.1f (gateway enqueue round trip)\n", len(samples), samples[(len(samples)-1)*95/100], samples[(len(samples)-1)*99/100], float64(len(samples))/time.Since(start).Seconds())
	return nil
}

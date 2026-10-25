package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"

	"github.com/thanhbvha/go-common/examples/websocket/internal/exampleconfig"
	"github.com/thanhbvha/go-common/logger"
	commonnats "github.com/thanhbvha/go-common/nats"
	commonredis "github.com/thanhbvha/go-common/redis"
	echomw "github.com/thanhbvha/go-common/web/middleware/echo"
	fibermw "github.com/thanhbvha/go-common/web/middleware/fiber"
	ginmw "github.com/thanhbvha/go-common/web/middleware/gin"
	nethttpmw "github.com/thanhbvha/go-common/web/middleware/nethttp"
	wsEcho "github.com/thanhbvha/go-common/websocket/adapter/echo"
	wsFiber "github.com/thanhbvha/go-common/websocket/adapter/fiber"
	wsGin "github.com/thanhbvha/go-common/websocket/adapter/gin"
	wsNetHTTP "github.com/thanhbvha/go-common/websocket/adapter/nethttp"
	"github.com/thanhbvha/go-common/websocket/core"
	"github.com/thanhbvha/go-common/websocket/limiter"
	"github.com/thanhbvha/go-common/websocket/pubsub"
	natspubsub "github.com/thanhbvha/go-common/websocket/pubsub/nats"
	redispubsub "github.com/thanhbvha/go-common/websocket/pubsub/redis"
)

const (
	eventSessionInfo      = "session.info"
	eventSystemPing       = "system.ping"
	eventRoomJoin         = "room.join"
	eventRoomLeave        = "room.leave"
	eventChatSend         = "chat.send"
	eventUserNotify       = "user.notify"
	eventClusterBroadcast = "cluster.broadcast"
)

type application struct {
	manager       *core.Manager
	redisClient   *commonredis.Client
	natsClient    *commonnats.Client
	clientToken   string
	adminToken    string
	defaultTenant string
}

type roomPayload struct {
	RoomID string `json:"room_id"`
}

type chatPayload struct {
	RoomID string `json:"room_id"`
	Text   string `json:"text"`
}

type userNotificationPayload struct {
	UserID string `json:"user_id"`
	Text   string `json:"text"`
}

type broadcastPayload struct {
	Text string `json:"text"`
}

type demoAuthorizer struct{}

func (demoAuthorizer) CanSubscribe(_ context.Context, identity core.Identity, roomID string) error {
	if identity.UserID == "" || identity.TenantID == "" || !strings.HasPrefix(roomID, identity.TenantID+":") {
		return core.ErrUnauthorized
	}
	return nil
}

func (demoAuthorizer) CanPublish(_ context.Context, identity core.Identity, eventType string) error {
	if identity.UserID == "" || identity.TenantID == "" {
		return core.ErrUnauthorized
	}
	if eventType == eventClusterBroadcast && identity.AccountID != "admin" {
		return core.ErrUnauthorized
	}
	return nil
}

func main() {
	configPath := exampleconfig.Path(os.Args[1:])
	loaded, err := exampleconfig.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config %q: %v\n", configPath, err)
		os.Exit(1)
	}
	configFile := flag.String("config", configPath, "YAML or .env configuration file")
	stack := flag.String("stack", loaded.ServerStack, "HTTP stack: nethttp, fiber, gin, or echo")
	broker := flag.String("broker", loaded.ServerBroker, "Broker: standalone, redis, or nats")
	address := flag.String("addr", loaded.ServerAddr, "HTTP listen address")
	redisAddress := flag.String("redis", loaded.ServerRedis, "Redis host:port")
	natsURL := flag.String("nats", loaded.ServerNATS, "NATS URL")
	defaultTenant := flag.String("tenant", loaded.ServerTenant, "Default tenant when tenant_id is omitted")
	flag.Parse()
	_ = configFile

	app, err := newApplication(*broker, *redisAddress, *natsURL, *defaultTenant, loaded.ClientToken, loaded.AdminToken)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer app.close()

	logger.InfoAsync("WebSocket example initialized", "stack", *stack, "broker", *broker, "address", *address)

	switch *stack {
	case "nethttp":
		err = app.runNetHTTP(*address)
	case "fiber":
		err = app.runFiber(*address)
	case "gin":
		err = app.runGin(*address)
	case "echo":
		err = app.runEcho(*address)
	default:
		err = fmt.Errorf("invalid -stack %q; use nethttp, fiber, gin, or echo", *stack)
	}
	if err != nil {
		logger.ErrorAsync("WebSocket example stopped with error", "error", err)
		app.close()
		os.Exit(1)
	}
}

func newApplication(broker, redisAddress, natsURL, defaultTenant, clientToken, adminToken string) (*application, error) {
	if clientToken == "" {
		return nil, errors.New("client_token is required in config or WS_EXAMPLE_CLIENT_TOKEN")
	}
	if adminToken == "" {
		adminToken = clientToken
	}

	l := logger.New(logger.Options{Level: slog.LevelInfo, StdOut: true})
	logger.SetDefault(l)

	app := &application{clientToken: clientToken, adminToken: adminToken, defaultTenant: defaultTenant}
	bus, ownership, err := app.newPubSub(broker, redisAddress, natsURL)
	if err != nil {
		return nil, err
	}

	registry := core.NewEventRegistry()
	registerHandlers(registry)

	config := core.DefaultManagerConfig()
	config.PubSub = bus
	config.PubSubOwnership = ownership
	config.EventRegistry = registry
	config.Authorizer = demoAuthorizer{}
	config.Observer = &core.AtomicObserver{}
	config.MaxConnections = 20_000
	config.MaxShards = 64
	config.MaxShardConnections = 1_000
	config.SendQueueSize = 128
	config.InboundQueueSize = 1_024
	config.MaxMessageBytes = 16 * 1024
	config.MessagesPerSecond = 20
	config.MessageBurst = 50
	config.BytesPerSecond = 32 * 1024
	config.ByteBurst = 64 * 1024
	config.MaxPendingHandlers = 16
	config.MaxHandlerGoroutines = 64
	config.MaxRoomsPerConnection = 50
	config.SlowConsumerDropThreshold = 2
	config.MaxConnectionsPerUser = 5
	config.MaxConnectionsPerAccount = 100
	config.MaxConnectionsPerTenant = 5_000
	config.WriteWait = 5 * time.Second
	config.PongWait = 60 * time.Second
	config.PingPeriod = 50 * time.Second

	app.manager = core.NewManagerWithConfig(config)
	if readiness, ok := bus.(pubsub.WaitReadyProvider); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := readiness.WaitReady(ctx)
		cancel()
		if err != nil {
			app.close()
			return nil, fmt.Errorf("wait for %s broker readiness: %w", broker, err)
		}
	}
	return app, nil
}

func (a *application) newPubSub(broker, redisAddress, natsURL string) (pubsub.Manager, core.PubSubOwnership, error) {
	switch broker {
	case "standalone":
		return pubsub.NewNoopManager(), core.PubSubOwned, nil
	case "redis":
		host, port, ok := strings.Cut(redisAddress, ":")
		if !ok || host == "" || port == "" {
			return nil, core.PubSubBorrowed, fmt.Errorf("invalid Redis address %q", redisAddress)
		}
		redisConfig := commonredis.DefaultConfig()
		redisConfig.Host = host
		if _, err := fmt.Sscanf(port, "%d", &redisConfig.Port); err != nil {
			return nil, core.PubSubBorrowed, fmt.Errorf("invalid Redis port in %q: %w", redisAddress, err)
		}
		redisConfig.Logger = logger.Default()
		a.redisClient = commonredis.New(redisConfig)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := a.redisClient.Connect(ctx); err != nil {
			return nil, core.PubSubBorrowed, fmt.Errorf("connect Redis: %w", err)
		}
		return redispubsub.NewManagerWithClientAndPolicy(a.redisClient, pubsub.FailStartup), core.PubSubOwned, nil
	case "nats":
		natsConfig := commonnats.DefaultConfig()
		natsConfig.URLs = []string{natsURL}
		natsConfig.Logger = logger.Default()
		a.natsClient = commonnats.New(natsConfig)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := a.natsClient.Connect(ctx); err != nil {
			return nil, core.PubSubBorrowed, fmt.Errorf("connect NATS: %w", err)
		}
		return natspubsub.NewManagerWithClientAndPolicy(a.natsClient, pubsub.FailStartup), core.PubSubOwned, nil
	default:
		return nil, core.PubSubBorrowed, fmt.Errorf("invalid -broker %q; use standalone, redis, or nats", broker)
	}
}

func registerHandlers(registry *core.EventRegistry) {
	registry.Register(eventSessionInfo, handlerConfig(2*time.Second, 1024, func(_ context.Context, conn *core.Connection, _ core.IncomingMessage) error {
		return sendEvent(conn, "session.info.result", map[string]any{
			"identity":    conn.Identity(),
			"shard_id":    conn.GetShardID(),
			"request_id":  conn.GetRequestID(),
			"server_time": time.Now().UTC(),
		})
	}))

	registry.Register(eventSystemPing, handlerConfig(time.Second, 1024, func(_ context.Context, conn *core.Connection, _ core.IncomingMessage) error {
		return sendEvent(conn, "system.pong", map[string]any{"server_time": time.Now().UTC()})
	}))

	registry.Register(eventRoomJoin, handlerConfig(2*time.Second, 4*1024, func(ctx context.Context, conn *core.Connection, message core.IncomingMessage) error {
		var payload roomPayload
		if err := decodePayload(message.Data, &payload); err != nil {
			return sendError(conn, eventRoomJoin, err)
		}
		if err := conn.GetShard().JoinRoom(ctx, payload.RoomID, conn); err != nil {
			return sendError(conn, eventRoomJoin, err)
		}
		return sendEvent(conn, "room.joined", map[string]any{"room_id": payload.RoomID, "members": conn.GetShard().GetRoomSize(payload.RoomID)})
	}))

	registry.Register(eventRoomLeave, handlerConfig(2*time.Second, 4*1024, func(ctx context.Context, conn *core.Connection, message core.IncomingMessage) error {
		var payload roomPayload
		if err := decodePayload(message.Data, &payload); err != nil {
			return sendError(conn, eventRoomLeave, err)
		}
		if err := conn.GetShard().LeaveRoom(ctx, payload.RoomID, conn); err != nil {
			return sendError(conn, eventRoomLeave, err)
		}
		return sendEvent(conn, "room.left", map[string]any{"room_id": payload.RoomID})
	}))

	registry.Register(eventChatSend, handlerConfig(2*time.Second, 8*1024, func(_ context.Context, conn *core.Connection, message core.IncomingMessage) error {
		var payload chatPayload
		if err := decodePayload(message.Data, &payload); err != nil {
			return sendError(conn, eventChatSend, err)
		}
		if payload.Text == "" {
			return sendError(conn, eventChatSend, errors.New("text is required"))
		}
		outgoing, err := json.Marshal(core.OutgoingMessage{Type: "chat.message", RequestID: conn.GetRequestID(), Data: map[string]any{
			"room_id": payload.RoomID,
			"text":    payload.Text,
			"sender":  conn.Identity(),
			"sent_at": time.Now().UTC(),
		}})
		if err != nil {
			return err
		}
		conn.GetShard().SendToRoom(payload.RoomID, outgoing)
		conn.GetShard().BroadcastRoomMessage(payload.RoomID, outgoing)
		return nil
	}))

	registry.Register(eventUserNotify, handlerConfig(2*time.Second, 8*1024, func(_ context.Context, conn *core.Connection, message core.IncomingMessage) error {
		var payload userNotificationPayload
		if err := decodePayload(message.Data, &payload); err != nil {
			return sendError(conn, eventUserNotify, err)
		}
		outgoing, err := json.Marshal(core.OutgoingMessage{Type: "user.notification", RequestID: conn.GetRequestID(), Data: map[string]any{
			"text":    payload.Text,
			"sender":  conn.GetUserID(),
			"sent_at": time.Now().UTC(),
		}})
		if err != nil {
			return err
		}
		conn.GetShard().SendToUser(payload.UserID, outgoing)
		conn.GetShard().BroadcastUserNotification(payload.UserID, outgoing)
		return nil
	}))

	registry.Register(eventClusterBroadcast, handlerConfig(2*time.Second, 8*1024, func(_ context.Context, conn *core.Connection, message core.IncomingMessage) error {
		var payload broadcastPayload
		if err := decodePayload(message.Data, &payload); err != nil {
			return sendError(conn, eventClusterBroadcast, err)
		}
		outgoing, err := json.Marshal(core.OutgoingMessage{Type: "cluster.broadcast", RequestID: conn.GetRequestID(), Data: map[string]any{
			"text":    payload.Text,
			"sender":  conn.GetUserID(),
			"sent_at": time.Now().UTC(),
		}})
		if err != nil {
			return err
		}
		conn.GetShard().BroadcastGlobal(outgoing)
		return nil
	}))
}

func handlerConfig(timeout time.Duration, maxPayloadBytes int, handler core.ContextJobHandler) core.HandlerConfig {
	return core.HandlerConfig{Timeout: timeout, MaxPayloadBytes: maxPayloadBytes, Handler: handler}
}

func decodePayload(data []byte, destination any) error {
	if len(data) == 0 || string(data) == "null" {
		return errors.New("payload is required")
	}
	if err := json.Unmarshal(data, destination); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}
	return nil
}

func sendEvent(conn *core.Connection, eventType string, data any) error {
	if !conn.SendJSON(core.OutgoingMessage{Type: eventType, Data: data, RequestID: conn.GetRequestID()}) {
		return errors.New("outbound queue rejected message")
	}
	return nil
}

func sendError(conn *core.Connection, source string, err error) error {
	if !conn.SendJSON(core.OutgoingMessage{Type: "error", Data: map[string]any{"source": source, "message": err.Error()}, RequestID: conn.GetRequestID()}) {
		return errors.New("outbound queue rejected error response")
	}
	return nil
}

func (a *application) authenticate(userID, tenantID, accountID, sessionID, authorization string) (core.Identity, error) {
	if !validBearerToken(authorization, a.clientToken) {
		return core.Identity{}, errors.New("invalid bearer token")
	}
	if userID == "" {
		return core.Identity{}, errors.New("user_id is required")
	}
	if tenantID == "" {
		tenantID = a.defaultTenant
	}
	if accountID == "" {
		accountID = "member"
	}
	if sessionID == "" {
		sessionID = fmt.Sprintf("%s-%d", userID, time.Now().UnixNano())
	}
	return core.Identity{UserID: userID, TenantID: tenantID, AccountID: accountID, SessionID: sessionID, ExpiresAt: time.Now().Add(8 * time.Hour)}, nil
}

func validBearerToken(authorization, expected string) bool {
	scheme, token, ok := strings.Cut(authorization, " ")
	return ok && strings.EqualFold(scheme, "Bearer") && token != "" && subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
}

func authenticateDemo(userID, authorization string) (string, error) {
	expected := os.Getenv("WS_DEMO_TOKEN")
	if expected == "" || !validBearerToken(authorization, expected) {
		return "", errors.New("invalid demo token")
	}
	if userID == "" {
		return "", errors.New("user_id is required")
	}
	return userID, nil
}

func (a *application) adminAuthorized(authorization string) bool {
	return validBearerToken(authorization, a.adminToken)
}

func (a *application) adapterLimiters() (*limiter.RateLimiter, *limiter.ConnectionLimiter) {
	return limiter.NewRateLimiter(100, 200), limiter.NewConnectionLimiter(20)
}

func (a *application) runNetHTTP(address string) error {
	rateLimiter, connectionLimiter := a.adapterLimiters()
	handler := wsNetHTTP.NewHandler(wsNetHTTP.Config{
		Manager: a.manager, RateLimiter: rateLimiter, ConnectionLimiter: connectionLimiter,
		AuthenticateIdentity: func(r *http.Request) (core.Identity, error) {
			query := r.URL.Query()
			return a.authenticate(query.Get("user_id"), query.Get("tenant_id"), query.Get("account_id"), query.Get("session_id"), r.Header.Get("Authorization"))
		},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handler.HandleUpgrade)
	mux.HandleFunc("/health", handler.HandleHealthCheck)
	mux.HandleFunc("/admin/stats", a.netHTTPAdmin(handler.HandleStats))
	mux.HandleFunc("/admin/shard", a.netHTTPAdmin(handler.HandleShardManagement))
	server := &http.Server{Addr: address, Handler: nethttpmw.Recover(mux), ReadHeaderTimeout: 5 * time.Second}
	return a.serveHTTP(server)
}

func (a *application) netHTTPAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.adminAuthorized(r.Header.Get("Authorization")) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (a *application) runGin(address string) error {
	rateLimiter, connectionLimiter := a.adapterLimiters()
	handler := wsGin.NewHandler(wsGin.Config{
		Manager: a.manager, RateLimiter: rateLimiter, ConnectionLimiter: connectionLimiter,
		AuthenticateIdentity: func(c *gin.Context) (core.Identity, error) {
			return a.authenticate(c.Query("user_id"), c.Query("tenant_id"), c.Query("account_id"), c.Query("session_id"), c.GetHeader("Authorization"))
		},
	})
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(ginmw.Recover())
	router.GET("/ws", handler.HandleUpgrade)
	router.GET("/health", handler.HandleHealthCheck)
	admin := router.Group("/admin", func(c *gin.Context) {
		if !a.adminAuthorized(c.GetHeader("Authorization")) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		}
	})
	admin.GET("/stats", handler.HandleStats)
	admin.GET("/shard", handler.HandleShardManagement)
	return a.serveHTTP(&http.Server{Addr: address, Handler: router, ReadHeaderTimeout: 5 * time.Second})
}

func (a *application) runEcho(address string) error {
	rateLimiter, connectionLimiter := a.adapterLimiters()
	handler := wsEcho.NewHandler(wsEcho.Config{
		Manager: a.manager, RateLimiter: rateLimiter, ConnectionLimiter: connectionLimiter,
		AuthenticateIdentity: func(c echo.Context) (core.Identity, error) {
			return a.authenticate(c.QueryParam("user_id"), c.QueryParam("tenant_id"), c.QueryParam("account_id"), c.QueryParam("session_id"), c.Request().Header.Get("Authorization"))
		},
	})
	e := echo.New()
	e.HideBanner, e.HidePort = true, true
	e.Use(echomw.Recover())
	e.GET("/ws", handler.HandleUpgrade)
	e.GET("/health", handler.HandleHealthCheck)
	admin := e.Group("/admin", func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if !a.adminAuthorized(c.Request().Header.Get("Authorization")) {
				return c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			}
			return next(c)
		}
	})
	admin.GET("/stats", handler.HandleStats)
	admin.GET("/shard", handler.HandleShardManagement)
	errChan := make(chan error, 1)
	go func() { errChan <- e.Start(address) }()
	return a.waitAndShutdown(errChan, func(ctx context.Context) error { return e.Shutdown(ctx) })
}

func (a *application) runFiber(address string) error {
	rateLimiter, connectionLimiter := a.adapterLimiters()
	handler := wsFiber.NewHandler(wsFiber.Config{
		Manager: a.manager, RateLimiter: rateLimiter, ConnectionLimiter: connectionLimiter,
		AuthenticateIdentity: func(c *fiber.Ctx) (core.Identity, error) {
			return a.authenticate(c.Query("user_id"), c.Query("tenant_id"), c.Query("account_id"), c.Query("session_id"), c.Get("Authorization"))
		},
	})
	app := fiber.New(fiber.Config{ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second})
	app.Use(fibermw.Recover())
	app.Get("/ws", handler.HandleUpgrade)
	app.Get("/health", handler.HandleHealthCheck)
	admin := app.Group("/admin", func(c *fiber.Ctx) error {
		if !a.adminAuthorized(c.Get("Authorization")) {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "unauthorized"})
		}
		return c.Next()
	})
	admin.Get("/stats", handler.HandleStats)
	admin.Get("/shard", handler.HandleShardManagement)
	errChan := make(chan error, 1)
	go func() { errChan <- app.Listen(address) }()
	return a.waitAndShutdown(errChan, func(context.Context) error { return app.Shutdown() })
}

func (a *application) serveHTTP(server *http.Server) error {
	errChan := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errChan <- err
	}()
	return a.waitAndShutdown(errChan, server.Shutdown)
}

func (a *application) waitAndShutdown(serverErrors <-chan error, shutdownHTTP func(context.Context) error) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case err := <-serverErrors:
		return err
	case signalValue := <-signals:
		logger.InfoAsync("Shutdown signal received", "signal", signalValue.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := shutdownHTTP(ctx); err != nil {
		logger.ErrorAsync("HTTP shutdown failed", "error", err)
	}
	return a.manager.Shutdown(ctx)
}

func (a *application) close() {
	if a.manager != nil && a.manager.State() != core.StateStopped {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = a.manager.Shutdown(ctx)
		cancel()
	}
	if a.redisClient != nil {
		_ = a.redisClient.Close()
	}
	if a.natsClient != nil {
		_ = a.natsClient.Close()
	}
	logger.Close()
}

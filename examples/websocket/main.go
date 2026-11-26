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
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"
	gonats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"

	"github.com/thanhbvha/go-common/v2/examples/websocket/internal/exampleconfig"
	"github.com/thanhbvha/go-common/v2/logger"
	echomw "github.com/thanhbvha/go-common/v2/web/middleware/echo"
	fibermw "github.com/thanhbvha/go-common/v2/web/middleware/fiber"
	ginmw "github.com/thanhbvha/go-common/v2/web/middleware/gin"
	nethttpmw "github.com/thanhbvha/go-common/v2/web/middleware/nethttp"
	"github.com/thanhbvha/go-common/v2/web/response"
	wsEcho "github.com/thanhbvha/go-common/v2/websocket/adapter/echo"
	wsFiber "github.com/thanhbvha/go-common/v2/websocket/adapter/fiber"
	wsGin "github.com/thanhbvha/go-common/v2/websocket/adapter/gin"
	wsNetHTTP "github.com/thanhbvha/go-common/v2/websocket/adapter/nethttp"
	"github.com/thanhbvha/go-common/v2/websocket/core"
	"github.com/thanhbvha/go-common/v2/websocket/limiter"
	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
	natsrouting "github.com/thanhbvha/go-common/v2/websocket/routing/nats"
	redisrouting "github.com/thanhbvha/go-common/v2/websocket/routing/redis"
	"github.com/thanhbvha/go-common/v2/xerrors"
)

const (
	eventSessionInfo = "session.info"
	eventSystemPing  = "system.ping"
	eventRoomJoin    = "room.join"
	eventRoomLeave   = "room.leave"
	eventChatSend    = "chat.send"
	eventUserNotify  = "user.notify"
)

// application wires the v2 topology: Redis is the session registry, Redis or
// NATS carries targeted delivery, and every fan-out goes through routing.Router.
type application struct {
	limits        exampleconfig.Config
	manager       *core.Manager
	gateway       *routing.Gateway
	router        *routing.Router
	redis         *goredis.Client
	nats          *gonats.Conn
	clientToken   string
	adminToken    string
	defaultTenant string
	closeOnce     sync.Once
	closeErr      error
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
	broker := flag.String("broker", loaded.ServerBroker, "Targeted delivery transport between nodes: redis or nats (Redis always holds the session registry)")
	address := flag.String("addr", loaded.ServerAddr, "HTTP listen address")
	redisAddress := flag.String("redis", loaded.ServerRedis, "Redis host:port")
	natsURL := flag.String("nats", loaded.ServerNATS, "NATS URL")
	defaultTenant := flag.String("tenant", loaded.ServerTenant, "Default tenant when tenant_id is omitted")
	flag.Parse()
	_ = configFile

	startup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	app, err := newApplication(startup, *broker, *redisAddress, *natsURL, *defaultTenant, loaded)
	cancel()
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

func newApplication(ctx context.Context, broker, redisAddress, natsURL, defaultTenant string, limits exampleconfig.Config) (a *application, result error) {
	if limits.ClientToken == "" {
		return nil, errors.New("client_token is required in config or WS_EXAMPLE_CLIENT_TOKEN")
	}
	adminToken := limits.AdminToken
	if adminToken == "" {
		adminToken = limits.ClientToken
	}
	if broker != "redis" && broker != "nats" {
		return nil, fmt.Errorf("invalid broker %q; use redis or nats", broker)
	}

	l := logger.New(logger.Options{Level: slog.LevelInfo, StdOut: true})
	logger.SetDefault(l)

	a = &application{limits: limits, clientToken: limits.ClientToken, adminToken: adminToken, defaultTenant: defaultTenant}
	defer func() {
		if result != nil {
			_ = a.close()
			a = nil
		}
	}()

	// Delivery messages between nodes are signed; cluster-grade validation
	// (ValidateProduction) refuses a gateway whose transport is unauthenticated.
	authenticator, err := pubsub.NewAuthenticator("example", map[string][]byte{"example": []byte(limits.BrokerKey)}, pubsub.AuthRequired)
	if err != nil {
		return a, errors.New("broker_key (WS_EXAMPLE_BROKER_KEY) must be at least 32 bytes; it authenticates delivery messages between nodes")
	}
	signed := routing.WithMessageAuthenticator(authenticator)

	redisOptions, err := redisOptions(redisAddress, limits.ServerRedisUsername, limits.ServerRedisPassword)
	if err != nil {
		return a, err
	}
	a.redis = goredis.NewClient(redisOptions)
	if err = a.redis.Ping(ctx).Err(); err != nil {
		// Never echo the address: it may embed a password.
		return a, fmt.Errorf("connect Redis: %w", err)
	}
	const namespace = "ws-example"
	registry := redisrouting.NewRegistry(a.redis, namespace)

	var delivery routing.DeliveryTransport
	if broker == "nats" {
		if a.nats, err = gonats.Connect(natsURL, gonats.Timeout(5*time.Second), gonats.Name("websocket-example")); err != nil {
			return a, fmt.Errorf("connect NATS %s: %w", natsURL, err)
		}
		delivery = natsrouting.NewTransport(a.nats, namespace, signed)
	} else {
		delivery = redisrouting.NewTransport(a.redis, namespace, signed)
	}

	if a.gateway, err = routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: delivery}); err != nil {
		return a, err
	}
	if err = a.gateway.Start(ctx); err != nil {
		return a, fmt.Errorf("start gateway: %w", err)
	}
	if a.router, err = routing.NewRouter(routing.RouterConfig{Registry: registry, Transport: delivery}); err != nil {
		return a, err
	}

	config := core.DefaultManagerConfig()
	config.Distributed = a.gateway // Borrowed; closed after the manager drains.
	config.EventRegistry = a.events()
	config.Authorizer = demoAuthorizer{}
	config.Observer = &core.AtomicObserver{}
	config.MaxShards = 64
	config.MaxShardConnections = 1_000
	config.MaxMessageBytes = 16 * 1024
	config.MessagesPerSecond = 20
	config.MessageBurst = 50
	config.BytesPerSecond = 32 * 1024
	config.ByteBurst = 64 * 1024
	config.MaxPendingHandlers = 16
	config.MaxRoomsPerConnection = 50
	config.SlowConsumerDropThreshold = 2
	config.WriteWait = 5 * time.Second
	config.PongWait = 60 * time.Second
	config.PingPeriod = 50 * time.Second
	limits.ApplyManagerLimits(&config)
	if a.manager, err = core.NewProductionManager(config); err != nil {
		return a, err
	}
	return a, nil
}

// redisOptions accepts a plain host:port or a URL (redis://user:pass@host:6379/0,
// rediss:// for TLS). Credentials may also come from the separate username and
// password settings, which override the URL so secrets can live in the environment.
func redisOptions(address, username, password string) (*goredis.Options, error) {
	options := &goredis.Options{Addr: address}
	if strings.Contains(address, "://") {
		var err error
		if options, err = goredis.ParseURL(address); err != nil {
			return nil, errors.New("invalid server_redis URL")
		}
	}
	if username != "" {
		options.Username = username
	}
	if password != "" {
		options.Password = password
	}
	return options, nil
}

// events registers the demo protocol. All fan-out goes through routing.Router,
// so rooms and user notifications work across shards and across nodes.
func (a *application) events() *core.EventRegistry {
	registry := core.NewEventRegistry()
	authz := demoAuthorizer{}
	register := func(event string, maxPayload int, handler core.ContextJobHandler) {
		registry.Register(event, core.HandlerConfig{Timeout: 3 * time.Second, MaxPayloadBytes: maxPayload, Handler: handler})
	}
	// deliver maps a Router outcome to an error so the client sees one failure shape.
	deliver := func(result routing.DeliveryResult, err error) error {
		if err == nil && result.Retryable() {
			err = routing.ErrUnavailable
		}
		return err
	}

	register(eventSessionInfo, 1024, func(_ context.Context, conn *core.Connection, _ core.IncomingMessage) error {
		return sendEvent(conn, "session.info.result", map[string]any{
			"identity":    conn.Identity(),
			"shard_id":    conn.GetShardID(),
			"request_id":  conn.GetRequestID(),
			"server_time": time.Now().UTC(),
		})
	})
	register(eventSystemPing, 1024, func(_ context.Context, conn *core.Connection, _ core.IncomingMessage) error {
		return sendEvent(conn, "system.pong", map[string]any{"server_time": time.Now().UTC()})
	})
	register(eventRoomJoin, 4*1024, func(ctx context.Context, conn *core.Connection, message core.IncomingMessage) error {
		var payload roomPayload
		if err := decodePayload(message.Data, &payload); err != nil {
			return sendError(conn, eventRoomJoin, err)
		}
		if err := conn.GetShard().JoinRoom(ctx, payload.RoomID, conn); err != nil {
			return sendError(conn, eventRoomJoin, err)
		}
		return sendEvent(conn, "room.joined", map[string]any{"room_id": payload.RoomID})
	})
	register(eventRoomLeave, 4*1024, func(ctx context.Context, conn *core.Connection, message core.IncomingMessage) error {
		var payload roomPayload
		if err := decodePayload(message.Data, &payload); err != nil {
			return sendError(conn, eventRoomLeave, err)
		}
		if err := conn.GetShard().LeaveRoom(ctx, payload.RoomID, conn); err != nil {
			return sendError(conn, eventRoomLeave, err)
		}
		return sendEvent(conn, "room.left", map[string]any{"room_id": payload.RoomID})
	})
	register(eventChatSend, 8*1024, func(ctx context.Context, conn *core.Connection, message core.IncomingMessage) error {
		var payload chatPayload
		if err := decodePayload(message.Data, &payload); err != nil {
			return sendError(conn, eventChatSend, err)
		}
		if payload.Text == "" {
			return sendError(conn, eventChatSend, xerrors.New("TEXT_REQUIRED", "Text is required", http.StatusBadRequest))
		}
		identity := conn.Identity()
		if err := authz.CanSubscribe(ctx, identity, payload.RoomID); err != nil {
			return sendError(conn, eventChatSend, err)
		}
		outgoing, err := json.Marshal(core.OutgoingMessage{Type: "chat.message", RequestID: conn.GetRequestID(), Data: map[string]any{
			"room_id": payload.RoomID,
			"text":    payload.Text,
			"sender":  identity,
			"sent_at": time.Now().UTC(),
		}})
		if err != nil {
			return err
		}
		if err = deliver(a.router.SendToRoom(ctx, identity.TenantID, payload.RoomID, outgoing, routing.NewID())); err != nil {
			return sendError(conn, eventChatSend, err)
		}
		return nil
	})
	register(eventUserNotify, 8*1024, func(ctx context.Context, conn *core.Connection, message core.IncomingMessage) error {
		var payload userNotificationPayload
		if err := decodePayload(message.Data, &payload); err != nil {
			return sendError(conn, eventUserNotify, err)
		}
		if payload.UserID == "" {
			return sendError(conn, eventUserNotify, xerrors.New("USER_REQUIRED", "user_id is required", http.StatusBadRequest))
		}
		outgoing, err := json.Marshal(core.OutgoingMessage{Type: "user.notification", RequestID: conn.GetRequestID(), Data: map[string]any{
			"text":    payload.Text,
			"sender":  conn.GetUserID(),
			"sent_at": time.Now().UTC(),
		}})
		if err != nil {
			return err
		}
		// The tenant always comes from the verified identity, never the payload.
		if err = deliver(a.router.SendToUser(ctx, conn.Identity().TenantID, payload.UserID, outgoing, routing.NewID())); err != nil {
			return sendError(conn, eventUserNotify, err)
		}
		return nil
	})
	return registry
}

func decodePayload(data []byte, destination any) error {
	if len(data) == 0 || string(data) == "null" {
		return xerrors.New("PAYLOAD_REQUIRED", "Payload is required", http.StatusBadRequest)
	}
	if err := json.Unmarshal(data, destination); err != nil {
		return xerrors.Wrap(err, "INVALID_PAYLOAD", "Invalid event payload", http.StatusBadRequest)
	}
	return nil
}

func sendEvent(conn *core.Connection, eventType string, data any) error {
	if !conn.SendJSON(core.OutgoingMessage{Type: eventType, Data: data, RequestID: conn.GetRequestID()}) {
		return errors.New("outbound queue rejected message")
	}
	return nil
}

type errorEventSender interface {
	SendJSON(any) bool
	GetRequestID() string
}

// eventErrorPayload is the complete public error contract. Diagnostic causes,
// fields, and stacks are never serialized into the WebSocket envelope.
type eventErrorPayload struct {
	Source  string `json:"source"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func publicEventError(source string, err error, requestID string) core.OutgoingMessage {
	publicErr := err
	switch {
	case errors.Is(err, core.ErrUnauthorized):
		publicErr = xerrors.Wrap(err, "FORBIDDEN", "You do not have permission to perform this operation", http.StatusForbidden)
	case errors.Is(err, core.ErrRoomLimit):
		publicErr = xerrors.Wrap(err, "ROOM_LIMIT", "Room subscription limit reached", http.StatusTooManyRequests)
	case errors.Is(err, core.ErrInvalidPayload):
		publicErr = xerrors.Wrap(err, "INVALID_PAYLOAD", "Invalid event payload", http.StatusBadRequest)
	case errors.Is(err, core.ErrPayloadTooLarge):
		publicErr = xerrors.Wrap(err, "PAYLOAD_TOO_LARGE", "Event payload exceeds the size limit", http.StatusRequestEntityTooLarge)
	case errors.Is(err, core.ErrConnectionInactive), errors.Is(err, core.ErrConnectionClosed):
		publicErr = xerrors.Wrap(err, "CONNECTION_INACTIVE", "Connection is no longer active", http.StatusConflict)
	case errors.Is(err, context.DeadlineExceeded):
		publicErr = xerrors.Wrap(err, "REQUEST_TIMEOUT", "Request timed out", http.StatusRequestTimeout)
	}
	// Explicit xerrors 4xx messages are public application validation errors.
	// Unknown errors and all 5xx details have one stable public fallback.
	public := response.FromError(publicErr, requestID)
	if public.Code < 400 || public.Code >= 500 {
		public = response.NewHTTPError(http.StatusInternalServerError, "INTERNAL_ERROR", "", requestID)
	}
	return core.OutgoingMessage{Type: "error", Data: eventErrorPayload{Source: source, Code: public.Status, Message: public.Message}, RequestID: requestID}
}

func sendError(conn errorEventSender, source string, err error) error {
	if err == nil {
		err = errors.New("websocket: missing event failure cause")
	}
	logger.ErrorAsync("WebSocket event failed", "source", source, "request_id", conn.GetRequestID(), "error", err)
	if !conn.SendJSON(publicEventError(source, err, conn.GetRequestID())) {
		return errors.Join(err, errors.New("outbound queue rejected error response"))
	}
	// Preserve the diagnostic cause for core EventError/trace accounting after
	// the safe client response has been queued.
	return err
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

func (a *application) adminAuthorized(authorization string) bool {
	return validBearerToken(authorization, a.adminToken)
}

func (a *application) adapterLimiters() (*limiter.KeyedRateLimiter, *limiter.ConnectionLimiter) {
	limits := a.limits
	if limits.MaxConnectionsPerIP == 0 {
		limits = exampleconfig.Default()
	}
	return limiter.NewKeyedRateLimiter(limits.UpgradeRate, limits.UpgradeBurst, 0, 0), limiter.NewConnectionLimiter(limits.MaxConnectionsPerIP)
}

func (a *application) runNetHTTP(address string) error {
	rateLimiter, connectionLimiter := a.adapterLimiters()
	handler := wsNetHTTP.NewHandler(wsNetHTTP.Config{
		Manager: a.manager, IPRateLimiter: rateLimiter, ConnectionLimiter: connectionLimiter, AdminToken: a.adminToken,
		AuthenticateIdentity: func(r *http.Request) (core.Identity, error) {
			query := r.URL.Query()
			return a.authenticate(query.Get("user_id"), query.Get("tenant_id"), query.Get("account_id"), query.Get("session_id"), r.Header.Get("Authorization"))
		},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handler.HandleUpgrade)
	mux.HandleFunc("/livez", handler.HandleLive)
	mux.HandleFunc("/readyz", handler.HandleReady)
	mux.HandleFunc("/health", handler.HandleReady) // Kept for existing probes and scripts.
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
		Manager: a.manager, IPRateLimiter: rateLimiter, ConnectionLimiter: connectionLimiter, AdminToken: a.adminToken,
		AuthenticateIdentity: func(c *gin.Context) (core.Identity, error) {
			return a.authenticate(c.Query("user_id"), c.Query("tenant_id"), c.Query("account_id"), c.Query("session_id"), c.GetHeader("Authorization"))
		},
	})
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(ginmw.Recover())
	router.GET("/ws", handler.HandleUpgrade)
	router.GET("/livez", handler.HandleLive)
	router.GET("/readyz", handler.HandleReady)
	router.GET("/health", handler.HandleReady) // Kept for existing probes and scripts.
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
		Manager: a.manager, IPRateLimiter: rateLimiter, ConnectionLimiter: connectionLimiter, AdminToken: a.adminToken,
		AuthenticateIdentity: func(c echo.Context) (core.Identity, error) {
			return a.authenticate(c.QueryParam("user_id"), c.QueryParam("tenant_id"), c.QueryParam("account_id"), c.QueryParam("session_id"), c.Request().Header.Get("Authorization"))
		},
	})
	e := echo.New()
	e.HideBanner, e.HidePort = true, true
	e.Use(echomw.Recover())
	e.GET("/ws", handler.HandleUpgrade)
	e.GET("/livez", handler.HandleLive)
	e.GET("/readyz", handler.HandleReady)
	e.GET("/health", handler.HandleReady) // Kept for existing probes and scripts.
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
		Manager: a.manager, IPRateLimiter: rateLimiter, ConnectionLimiter: connectionLimiter, AdminToken: a.adminToken,
		AuthenticateIdentity: func(c *fiber.Ctx) (core.Identity, error) {
			return a.authenticate(c.Query("user_id"), c.Query("tenant_id"), c.Query("account_id"), c.Query("session_id"), c.Get("Authorization"))
		},
	})
	app := fiber.New(fiber.Config{ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second})
	app.Use(fibermw.Recover())
	app.Get("/ws", handler.HandleUpgrade)
	app.Get("/livez", handler.HandleLive)
	app.Get("/readyz", handler.HandleReady)
	app.Get("/health", handler.HandleReady) // Kept for existing probes and scripts.
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

	return a.shutdown(shutdownHTTP)
}

func (a *application) shutdown(shutdownHTTP func(context.Context) error) error {
	httpCtx, cancelHTTP := context.WithTimeout(context.Background(), 15*time.Second)
	httpErr := shutdownHTTP(httpCtx)
	cancelHTTP()
	managerCtx, cancelManager := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelManager()
	return errors.Join(httpErr, a.manager.Shutdown(managerCtx))
}

// close is idempotent. Order matters: drain sockets first, then close the
// gateway (which releases leases), then the broker connections.
func (a *application) close() error {
	a.closeOnce.Do(func() {
		step := func(close func(context.Context) error) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			a.closeErr = errors.Join(a.closeErr, close(ctx))
		}
		if a.manager != nil && a.manager.State() != core.StateStopped {
			step(a.manager.Shutdown)
		}
		if a.gateway != nil {
			step(a.gateway.Close)
		}
		if a.nats != nil {
			a.nats.Close()
		}
		if a.redis != nil {
			a.closeErr = errors.Join(a.closeErr, a.redis.Close())
		}
		logger.Close()
	})
	return a.closeErr
}

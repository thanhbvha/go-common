package main

import (
	"context"
	"crypto/subtle"
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

	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/nats"
	"github.com/thanhbvha/go-common/redis"
	echomw "github.com/thanhbvha/go-common/web/middleware/echo"
	fibermw "github.com/thanhbvha/go-common/web/middleware/fiber"
	ginmw "github.com/thanhbvha/go-common/web/middleware/gin"
	nethttpmw "github.com/thanhbvha/go-common/web/middleware/nethttp"
	wsEcho "github.com/thanhbvha/go-common/websocket/adapter/echo"
	wsFiber "github.com/thanhbvha/go-common/websocket/adapter/fiber"
	wsGin "github.com/thanhbvha/go-common/websocket/adapter/gin"
	wsNetHTTP "github.com/thanhbvha/go-common/websocket/adapter/nethttp"
	"github.com/thanhbvha/go-common/websocket/core"
	"github.com/thanhbvha/go-common/websocket/pubsub"
	redispubsub "github.com/thanhbvha/go-common/websocket/pubsub/redis"
)

func main() {
	stack := flag.String("stack", "nethttp", "HTTP stack: nethttp, fiber, gin, or echo")
	flag.Parse()
	fmt.Println("=== Clustered WebSocket Module Examples ===")

	// 1. Setup shared infrastructure (Logger, Redis, NATS, Event Handlers)
	SetupCore()

	// 2. Start the selected HTTP adapter. All adapters bind to port 8080.
	switch *stack {
	case "nethttp":
		RunNetHTTPExample()
	case "fiber":
		RunFiberExample()
	case "gin":
		RunGinExample()
	case "echo":
		RunEchoExample()
	default:
		fmt.Fprintln(os.Stderr, "invalid -stack; use nethttp, fiber, gin, or echo")
		os.Exit(2)
	}
}

// SetupCore initializes the shared architecture components that are framework-agnostic.
func SetupCore() {
	// 1. Bootstrap structured asynchronous logger
	l := logger.New(logger.Options{
		Level:  slog.LevelDebug,
		StdOut: true,
	})
	logger.SetDefault(l)

	logger.InfoAsync("Bootstrapping Clustered WebSocket Service...")

	// 2. Initialize process-wide default Redis client (For Pub/Sub)
	redisCfg := redis.DefaultConfig()
	redisCfg.Host = "localhost"
	redisCfg.Port = 6379
	redisCfg.MaxConnRetries = 1

	redisClient := redis.New(redisCfg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := redisClient.Connect(ctx); err != nil {
		logger.WarnAsync("Redis unavailable, running WebSocket in standalone loopback mode", "error", err)
	} else {
		redis.SetDefault(redisClient)
		// Inject the Redis PubSub Manager into the WebSocket core for clustered message routing
		pubsub.SetGlobalManager(redispubsub.NewManagerWithClient(redisClient))
		logger.InfoAsync("Redis clustered pub/sub engine connected successfully")
	}

	// [NATS Alternative]
	// If you wanted to use NATS instead of Redis for PubSub:
	// import natspubsub "github.com/thanhbvha/go-common/websocket/pubsub/nats"
	// natsClient := nats.New(...)
	// pubsub.SetGlobalManager(natspubsub.NewManagerWithClient(natsClient))

	// 3. Register custom event handlers for business logic
	core.RegisterHandler("chat_message", func(conn *core.Connection, msg core.IncomingMessage) error {
		logger.InfoAsync("Received chat message event", "userID", conn.GetUserID(), "payloadBytes", len(msg.Data))

		// Echo message back to sender
		conn.SendJSON(core.OutgoingMessage{
			Type: "chat_echo",
			Data: map[string]interface{}{
				"sender":  conn.GetUserID(),
				"payload": string(msg.Data),
				"sent_at": time.Now(),
			},
		})
		return nil
	})
}

// =====================================================================
// 1. Standard net/http WebSocket Adapter Example
// =====================================================================
func RunNetHTTPExample() {
	logger.InfoAsync("Starting net/http WebSocket Adapter...")

	handler := wsNetHTTP.NewHandler(wsNetHTTP.Config{
		Authenticate: func(r *http.Request) (string, error) {
			return authenticateDemo(r.URL.Query().Get("user_id"), r.Header.Get("Authorization"))
		},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handler.HandleUpgrade)
	mux.HandleFunc("/health", handler.HandleHealthCheck)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           nethttpmw.Recover(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.ErrorAsync("Fatal error in net/http server listen", "error", err)
		}
	}()

	waitForShutdown(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.ErrorAsync("Failed to shut down net/http server", "error", err)
		}
	})
}

// =====================================================================
// 2. Fiber WebSocket Adapter Example
// =====================================================================
func RunFiberExample() {
	logger.InfoAsync("Starting Fiber WebSocket Adapter...")

	adapterConfig := wsFiber.Config{
		Authenticate: func(c *fiber.Ctx) (string, error) {
			return authenticateDemo(c.Query("user_id"), c.Get("Authorization"))
		},
	}

	handler := wsFiber.NewHandler(adapterConfig)
	server := wsFiber.NewServer(8080, handler)
	server.App().Use(fibermw.Recover())
	server.SetupRoutes()

	if err := server.Start(); err != nil {
		logger.ErrorAsync("Fatal error starting WebSocket server", "error", err)
		os.Exit(1)
	}

	waitForShutdown(func() {
		server.Shutdown()
	})
}

// =====================================================================
// 3. Gin WebSocket Adapter Example
// =====================================================================
func RunGinExample() {
	logger.InfoAsync("Starting Gin WebSocket Adapter...")

	handler := wsGin.NewHandler(wsGin.Config{
		Authenticate: func(c *gin.Context) (string, error) {
			return authenticateDemo(c.Query("user_id"), c.GetHeader("Authorization"))
		},
	})

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(ginmw.Recover())

	r.GET("/ws", handler.HandleUpgrade)
	r.GET("/health", handler.HandleHealthCheck)

	server := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.ErrorAsync("Fatal error in Gin server listen", "error", err)
			os.Exit(1)
		}
	}()

	waitForShutdown(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	})
}

// =====================================================================
// 4. Echo WebSocket Adapter Example
// =====================================================================
func RunEchoExample() {
	logger.InfoAsync("Starting Echo WebSocket Adapter...")

	handler := wsEcho.NewHandler(wsEcho.Config{
		Authenticate: func(c echo.Context) (string, error) {
			return authenticateDemo(c.QueryParam("user_id"), c.Request().Header.Get("Authorization"))
		},
	})

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Use(echomw.Recover())

	e.GET("/ws", handler.HandleUpgrade)
	e.GET("/health", handler.HandleHealthCheck)

	go func() {
		if err := e.Start(":8080"); err != nil && err != http.ErrServerClosed {
			logger.ErrorAsync("Fatal error in Echo server listen", "error", err)
			os.Exit(1)
		}
	}()

	waitForShutdown(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Shutdown(shutdownCtx)
	})
}

func authenticateDemo(userID, authorization string) (string, error) {
	expectedToken := os.Getenv("WS_DEMO_TOKEN")
	if expectedToken == "" {
		return "", fmt.Errorf("WS_DEMO_TOKEN is required")
	}
	scheme, token, ok := strings.Cut(authorization, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" ||
		subtle.ConstantTimeCompare([]byte(token), []byte(expectedToken)) != 1 {
		return "", fmt.Errorf("invalid demo token")
	}
	if userID == "" {
		return "", fmt.Errorf("user_id query parameter is required")
	}
	return userID, nil
}

// waitForShutdown blocks until SIGINT/SIGTERM, shuts down the HTTP server, and terminates the global websocket manager.
func waitForShutdown(shutdownHttp func()) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.InfoAsync("Initiating graceful shutdown sequence...")

	// 1. Shutdown HTTP Server
	shutdownHttp()

	// 2. Terminate WebSocket Manager (CRITICAL)
	// This cascades the shutdown signal to all Shards to disconnect clients cleanly.
	core.GetGlobalManager().Shutdown()
	logger.InfoAsync("Service shutdown completed gracefully.")

	// Close resources
	logger.Close()
	redis.Close()
	nats.Close()
}

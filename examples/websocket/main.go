package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"

	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/nats"
	"github.com/thanhbvha/go-common/redis"
	wsEcho "github.com/thanhbvha/go-common/websocket/adapter/echo"
	wsFiber "github.com/thanhbvha/go-common/websocket/adapter/fiber"
	wsGin "github.com/thanhbvha/go-common/websocket/adapter/gin"
	"github.com/thanhbvha/go-common/websocket/core"
)

func main() {
	fmt.Println("=== Clustered WebSocket Module Examples ===")

	// 1. Setup shared infrastructure (Logger, Redis, NATS, Event Handlers)
	SetupCore()

	// 2. Uncomment the framework example you want to run.
	// NOTE: Only run ONE example at a time since they all bind to port 8080!
	RunFiberExample()
	// RunGinExample()
	// RunEchoExample()
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
		logger.InfoAsync("Redis clustered pub/sub engine connected successfully")
	}

	// 3. Register custom event handlers for business logic
	core.RegisterHandler("chat_message", func(conn *core.Connection, msg core.IncomingMessage) error {
		logger.InfoAsync("Received chat message event", "userID", conn.GetUserID(), "payload", string(msg.Data))

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
// 1. Fiber WebSocket Adapter Example
// =====================================================================
func RunFiberExample() {
	logger.InfoAsync("Starting Fiber WebSocket Adapter...")

	adapterConfig := wsFiber.Config{
		Authenticate: func(c *fiber.Ctx) (string, error) {
			userID := c.Query("user_id")
			if userID == "" {
				return "", fmt.Errorf("user_id query parameter is required")
			}
			return userID, nil
		},
		PubSubAdapter: "redis",
	}

	handler := wsFiber.NewHandler(adapterConfig)
	server := wsFiber.NewServer(8080, handler)
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
// 2. Gin WebSocket Adapter Example
// =====================================================================
func RunGinExample() {
	logger.InfoAsync("Starting Gin WebSocket Adapter...")

	handler := wsGin.NewHandler(wsGin.Config{
		Authenticate: func(c *gin.Context) (string, error) {
			userID := c.Query("user_id")
			if userID == "" {
				return "", fmt.Errorf("user_id query parameter is required")
			}
			return userID, nil
		},
		PubSubAdapter: "redis",
	})

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/ws", handler.HandleUpgrade)
	r.GET("/stats", handler.HandleStats)
	r.GET("/shard", handler.HandleShardManagement)
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
// 3. Echo WebSocket Adapter Example
// =====================================================================
func RunEchoExample() {
	logger.InfoAsync("Starting Echo WebSocket Adapter...")

	handler := wsEcho.NewHandler(wsEcho.Config{
		Authenticate: func(c echo.Context) (string, error) {
			userID := c.QueryParam("user_id")
			if userID == "" {
				return "", fmt.Errorf("user_id query parameter is required")
			}
			return userID, nil
		},
		PubSubAdapter: "redis",
	})

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.GET("/ws", handler.HandleUpgrade)
	e.GET("/stats", handler.HandleStats)
	e.GET("/shard", handler.HandleShardManagement)
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

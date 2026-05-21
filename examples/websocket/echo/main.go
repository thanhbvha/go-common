// Package main illustrates how to boot and run the high-performance clustered WebSocket service using the Echo adapter.
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

	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/redis"
	wsEcho "github.com/thanhbvha/go-common/websocket/adapter/echo"
	"github.com/thanhbvha/go-common/websocket/core"
)

func main() {
	// 1. Bootstrap structured asynchronous logger
	l := logger.New(logger.Options{
		Level:  slog.LevelDebug,
		StdOut: true,
		File: &logger.FileOptions{
			Path:       "logs/websocket_echo.log",
			MaxSizeMB:  50,
			MaxBackups: 3,
			MaxAgeDays: 7,
			Compress:   true,
		},
	})
	logger.SetDefault(l)
	defer logger.Close()

	logger.InfoAsync("Bootstrapping Clustered WebSocket Service using Echo adapter...")

	// 2. Initialize process-wide default Redis client (Optional - falls back to standalone loopback if omitted)
	redisCfg := redis.DefaultConfig()
	redisCfg.Host = "localhost"
	redisCfg.Port = 6379
	redisCfg.MaxConnRetries = 1 // Don't block startup forever if Redis is unavailable locally

	redisClient := redis.New(redisCfg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := redisClient.Connect(ctx); err != nil {
		logger.WarnAsync("Redis unavailable on localhost:6379, running WebSocket in standalone loopback mode", "error", err)
	} else {
		redis.SetDefault(redisClient)
		defer redis.Close()
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

	// 4. Configure and instantiate the Echo WebSocket adapter
	// Note: We use the variadic config parameters, falling back to all optimized defaults!
	handler := wsEcho.NewHandler(wsEcho.Config{
		Authenticate: func(c echo.Context) (string, error) {
			userID := c.QueryParam("user_id")
			if userID == "" {
				return "", fmt.Errorf("user_id query parameter is required")
			}
			return userID, nil
		},
	})

	// 5. Initialize Echo Engine and setup routes
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.GET("/ws", handler.HandleUpgrade)
	e.GET("/stats", handler.HandleStats)
	e.GET("/shard", handler.HandleShardManagement)
	e.GET("/health", handler.HandleHealthCheck)

	// 6. Configure HTTP Server settings on Echo engine
	e.Server.ReadTimeout = 10 * time.Second
	e.Server.WriteTimeout = 10 * time.Second
	e.Server.IdleTimeout = 120 * time.Second

	// Start server in background
	go func() {
		logger.InfoAsync("Echo HTTP/WebSocket Server listening on :8080")
		if err := e.Start(":8080"); err != nil && err != http.ErrServerClosed {
			logger.ErrorAsync("Fatal error in Echo server listen", "error", err)
			os.Exit(1)
		}
	}()

	// 7. Handle graceful shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.InfoAsync("Initiating graceful shutdown sequence...")
	
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := e.Shutdown(shutdownCtx); err != nil {
		logger.ErrorAsync("Error during Echo server shutdown", "error", err)
	}

	// Terminate active manager routines and connection pools
	core.GetGlobalManager().Shutdown()
	logger.InfoAsync("Service shutdown completed gracefully.")
}

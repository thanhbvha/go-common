// Package main illustrates how to boot and run the high-performance clustered WebSocket service using the Fiber adapter.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/redis"
	wsFiber "github.com/thanhbvha/go-common/websocket/adapter/fiber"
	"github.com/thanhbvha/go-common/websocket/ws"
)

func main() {
	// 1. Bootstrap structured asynchronous logger
	l := logger.New(logger.Options{
		Level:  slog.LevelDebug,
		StdOut: true,
		File: &logger.FileOptions{
			Path:       "logs/websocket_fiber.log",
			MaxSizeMB:  50,
			MaxBackups: 3,
			MaxAgeDays: 7,
			Compress:   true,
		},
	})
	logger.SetDefault(l)
	defer logger.Close()

	logger.InfoAsync("Bootstrapping Clustered WebSocket Service using Fiber adapter...")

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
	ws.RegisterHandler("chat_message", func(conn *ws.Connection, msg ws.IncomingMessage) error {
		logger.InfoAsync("Received chat message event", "userID", conn.GetUserID(), "payload", string(msg.Data))

		// Echo message back to sender
		conn.SendJSON(ws.OutgoingMessage{
			Type: "chat_echo",
			Data: map[string]interface{}{
				"sender":  conn.GetUserID(),
				"payload": string(msg.Data),
				"sent_at": time.Now(),
			},
		})
		return nil
	})

	// 4. Configure the Fiber WebSocket adapter
	adapterConfig := wsFiber.Config{
		Authenticate: func(c *fiber.Ctx) (string, error) {
			userID := c.Query("user_id")
			if userID == "" {
				return "", fmt.Errorf("user_id query parameter is required")
			}
			return userID, nil
		},
	}

	// 5. Instantiate and initialize HTTP and WS routing server wrapper
	handler := wsFiber.NewHandler(adapterConfig)
	server := wsFiber.NewServer(8080, handler)
	server.SetupRoutes()

	// 6. Start the server
	if err := server.Start(); err != nil {
		logger.ErrorAsync("Fatal error starting WebSocket server", "error", err)
		os.Exit(1)
	}

	// 7. Handle graceful shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.InfoAsync("Initiating graceful shutdown sequence...")
	if err := server.Shutdown(); err != nil {
		logger.ErrorAsync("Error during server shutdown", "error", err)
	}

	// Terminate active manager routines and connection pools
	ws.GetGlobalManager().Shutdown()
	logger.InfoAsync("Service shutdown completed gracefully.")
}

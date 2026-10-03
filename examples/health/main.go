package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/health"
)

func main() {
	fmt.Println("=== Health Module Example ===")
	
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	// Create health checker
	h := health.New().
		WithVersion("v1.2.0").
		WithTimeout(2 * time.Second)

	// =====================================================================
	// 1. PostgreSQL Check (GORM)
	// =====================================================================
	// In production, you would use the built-in GORMChecker and pass your *gorm.DB instance:
	//   db, _ := gorm.Open(...)
	//   h.AddReadinessCheck("postgresql", health.GORMChecker(db))
	// 
	// Here we simulate the result:
	h.AddReadinessCheck("postgresql", health.CustomChecker(func(ctx context.Context) error {
		time.Sleep(100 * time.Millisecond) // Simulate DB latency
		return nil // OK
	}))

	// =====================================================================
	// 2. Redis Check (go-redis)
	// =====================================================================
	// In production, you would pass your redis.Client to the RedisChecker:
	//   rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	//   h.AddReadinessCheck("redis", health.RedisChecker(rdb))
	//
	// Simulate a Redis failure (to see the /ready endpoint return 503):
	h.AddReadinessCheck("redis", health.CustomChecker(func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond) // Simulate Redis latency
		return errors.New("dial tcp [::1]:6379: connect: connection refused")
	}))

	// =====================================================================
	// 3. MongoDB Check (Using CustomChecker)
	// =====================================================================
	// Since there is no built-in MongoChecker, you can create one using CustomChecker:
	//   client, _ := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	//   h.AddReadinessCheck("mongodb", health.CustomChecker(func(ctx context.Context) error {
	//       return client.Ping(ctx, readpref.Primary())
	//   }))
	h.AddReadinessCheck("mongodb", health.CustomChecker(func(ctx context.Context) error {
		time.Sleep(150 * time.Millisecond)
		return nil // OK
	}))

	// =====================================================================
	// 4. Liveness Check (Process/Memory)
	// =====================================================================
	// Liveness checks must be extremely fast (checking if the application is alive).
	// If they fail, Kubernetes will restart the container.
	h.AddLivenessCheck("memory", health.CustomChecker(func(ctx context.Context) error {
		return nil
	}))

	// Register endpoints
	app.Get("/health", health.FiberHandler(h))
	app.Get("/ready", health.FiberReadyHandler(h))
	app.Get("/live", health.FiberLiveHandler(h))

	// Start server in background
	go func() {
		if err := app.Listen(":3000"); err != nil {
			log.Fatal(err)
		}
	}()

	fmt.Println("Server running on :3000")
	fmt.Println("Waiting for server to start...")
	time.Sleep(1 * time.Second)

	// Simulate requests
	runRequest("GET", "http://localhost:3000/live")
	runRequest("GET", "http://localhost:3000/ready")
	runRequest("GET", "http://localhost:3000/health")
}

func runRequest(method, url string) {
	fmt.Printf("\n--- %s %s ---\n", method, url)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()
	
	fmt.Printf("Status: %d %s\n", resp.StatusCode, resp.Status)
	
	// Print JSON body
	var body = make([]byte, 1024)
	n, _ := resp.Body.Read(body)
	fmt.Printf("Body: %s\n", body[:n])
}

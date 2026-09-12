package main

import (
	"context"
	"fmt"

	"github.com/thanhbvha/go-common/redis"
)

func main() {
	fmt.Println("--- Redis Example ---")

	ctx := context.Background()

	// ==========================================
	// 1. SINGLE NODE CONFIGURATION
	// ==========================================
	// Use DefaultConfig() to get a production-ready baseline.
	cfg := redis.DefaultConfig()
	cfg.Mode = redis.ModeSingle
	cfg.Host = "localhost"
	cfg.Port = 6379
	cfg.Password = "" // Set if required
	cfg.DB = 0

	// Note: For Cluster mode, you would use:
	// cfg.Mode = redis.ModeCluster
	// cfg.ClusterAddrs = []string{"node1:6379", "node2:6379"}

	// ==========================================
	// 2. INITIALIZATION
	// ==========================================
	// MustConnect will panic if the connection fails (e.g., redis is offline).
	// For graceful handling, use redis.New(cfg) followed by Connect(ctx).
	fmt.Println("Connecting to Redis (Ensure it is running locally on port 6379)...")
	rdb := redis.MustConnect(ctx, cfg)
	
	// Set the global default so other packages can use redis.Default()
	redis.SetDefault(rdb)

	// ALWAYS call Close() to cleanly shutdown the connection pool and health checks
	defer redis.Close()
	fmt.Println("Connected successfully!")

	// ==========================================
	// 3. BASIC USAGE (Native Client)
	// ==========================================
	// The wrapper provides a Native() method to access the underlying go-redis client.
	client := rdb.Native()
	
	err := client.Set(ctx, "example_key", "Hello from go-common", 0).Err()
	if err != nil {
		fmt.Printf("Failed to set key: %v\n", err)
		return
	}

	val, err := client.Get(ctx, "example_key").Result()
	if err != nil {
		fmt.Printf("Failed to get key: %v\n", err)
		return
	}

	fmt.Printf("Retrieved value: %s\n", val)
}

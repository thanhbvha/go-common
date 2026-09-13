package main

import (
	"context"
	"fmt"
	"time"

	"github.com/thanhbvha/go-common/redis"
)

func main() {
	fmt.Println("=== Redis Module Examples ===")

	// Uncomment the example you want to run:
	RunSingleNodeExample()
	// RunClusterModeExample()
	// RunSentinelModeExample()
}

// =====================================================================
// 1. Single Node Configuration
// =====================================================================
func RunSingleNodeExample() {
	fmt.Println("\n--- 1. Single Node Example ---")
	ctx := context.Background()

	// Use DefaultConfig() to get a production-ready baseline.
	cfg := redis.DefaultConfig()
	cfg.Mode = redis.ModeSingle
	cfg.Host = "localhost"
	cfg.Port = 6379
	cfg.Password = "" // Set if required
	cfg.DB = 0
	
	// MaxConnRetries ensures we don't panic immediately if Redis is booting
	cfg.MaxConnRetries = 3

	fmt.Println("Connecting to Redis (Ensure it is running locally on port 6379)...")
	
	// MustConnect will panic if the connection fails completely.
	// For graceful handling, use redis.New(cfg) followed by Connect(ctx).
	rdb := redis.MustConnect(ctx, cfg)
	
	// Set the global default so other packages can use redis.Default()
	redis.SetDefault(rdb)

	// ALWAYS call Close() to cleanly shutdown the connection pool and health checks
	defer redis.Close()
	fmt.Println("✅ Connected successfully!")

	// ── BASIC USAGE (Native Client) ──
	// The wrapper provides a Native() method to access the underlying go-redis client.
	client := rdb.Native()
	
	err := client.Set(ctx, "example_key", "Hello from Single Node", 10*time.Minute).Err()
	if err != nil {
		fmt.Printf("❌ Failed to set key: %v\n", err)
		return
	}

	val, err := client.Get(ctx, "example_key").Result()
	if err != nil {
		fmt.Printf("❌ Failed to get key: %v\n", err)
		return
	}

	fmt.Printf("📖 Retrieved value: %s\n", val)
}

// =====================================================================
// 2. Cluster Mode Configuration
// =====================================================================
func RunClusterModeExample() {
	fmt.Println("\n--- 2. Cluster Mode Example ---")
	ctx := context.Background()

	cfg := redis.DefaultConfig()
	cfg.Mode = redis.ModeCluster
	
	// Provide the seed nodes for the cluster
	cfg.ClusterAddrs = []string{
		"localhost:7000",
		"localhost:7001",
		"localhost:7002",
	}
	cfg.Password = "my-cluster-password"

	fmt.Println("Connecting to Redis Cluster (Will timeout if not running locally)...")
	
	// Use New() + Connect() to handle connection errors gracefully
	rdb := redis.New(cfg)
	if err := rdb.Connect(ctx); err != nil {
		fmt.Printf("❌ Cluster connection failed (expected if no local cluster): %v\n", err)
		return
	}
	defer rdb.Close()
	
	fmt.Println("✅ Connected to Cluster successfully!")
	
	client := rdb.Native()
	_ = client.Set(ctx, "cluster_key", "Hello from Cluster", 0).Err()
}

// =====================================================================
// 3. Sentinel Mode Configuration (High Availability)
// =====================================================================
func RunSentinelModeExample() {
	fmt.Println("\n--- 3. Sentinel Mode Example ---")
	ctx := context.Background()

	cfg := redis.DefaultConfig()
	cfg.Mode = redis.ModeSentinel
	
	// Configure Sentinel endpoints and Master name
	cfg.MasterName = "mymaster"
	cfg.SentinelAddrs = []string{
		"localhost:26379",
		"localhost:26380",
		"localhost:26381",
	}
	cfg.Password = "my-redis-password"
	cfg.DB = 0 // Sentinels support multiple logical DBs

	fmt.Println("Connecting to Redis Sentinel (Will timeout if not running locally)...")
	
	rdb := redis.New(cfg)
	if err := rdb.Connect(ctx); err != nil {
		fmt.Printf("❌ Sentinel connection failed (expected if no local sentinels): %v\n", err)
		return
	}
	defer rdb.Close()
	
	fmt.Println("✅ Connected to Sentinel Master successfully!")
	
	client := rdb.Native()
	_ = client.Set(ctx, "sentinel_key", "Hello from Sentinel Master", 0).Err()
}

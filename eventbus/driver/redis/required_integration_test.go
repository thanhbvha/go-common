//go:build integration

package redis

import (
	"context"
	"fmt"
	goredis "github.com/redis/go-redis/v9"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("EVENTBUS_INTEGRATION_REQUIRED") == "1" {
		single, cluster := os.Getenv("EVENTBUS_REDIS_ADDR"), os.Getenv("EVENTBUS_REDIS_CLUSTER_ADDRS")
		if single == "" || cluster == "" {
			fmt.Fprintln(os.Stderr, "required isolated Redis single/cluster addresses missing")
			os.Exit(1)
		}
		clients := []goredis.UniversalClient{goredis.NewClient(&goredis.Options{Addr: single}), goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: strings.Split(cluster, ",")})}
		for _, client := range clients {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := client.Ping(ctx).Err()
			cancel()
			_ = client.Close()
			if err != nil {
				fmt.Fprintln(os.Stderr, "required isolated Redis fixture unavailable:", err)
				os.Exit(1)
			}
		}
	}
	os.Exit(m.Run())
}

func redisIntegrationUnavailable(t *testing.T, fixture string, err error) {
	t.Helper()
	if os.Getenv("EVENTBUS_INTEGRATION_REQUIRED") == "1" {
		t.Fatalf("required %s fixture unavailable: %v", fixture, err)
	}
	t.Skipf("%s unavailable: %v", fixture, err)
}

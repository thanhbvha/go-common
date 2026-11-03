// Package redis adapts go-redis clients to health checks.
package redis

import (
	"context"
	"errors"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/v2/health"
)

func New(client goredis.UniversalClient) health.CheckFunc {
	return func(ctx context.Context) error {
		if client == nil {
			return errors.New("health/redis: nil client")
		}
		return client.Ping(ctx).Err()
	}
}

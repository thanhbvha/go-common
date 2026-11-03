// Package sql adapts SQL pools to health checks without a dependency on an ORM.
package sql

import (
	"context"
	"errors"
	"github.com/thanhbvha/go-common/v2/health"
)

type Pinger interface{ PingContext(context.Context) error }

func New(db Pinger) health.CheckFunc {
	return func(ctx context.Context) error {
		if db == nil {
			return errors.New("health/sql: nil pool")
		}
		return db.PingContext(ctx)
	}
}

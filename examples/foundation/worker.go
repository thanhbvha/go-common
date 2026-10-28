package main

import (
	"context"
	"time"

	"github.com/thanhbvha/go-common/foundation"
	"github.com/thanhbvha/go-common/logger"
)

func runWorker(ctx context.Context) error {
	app, err := foundation.New(foundation.Config{Name: "foundation-worker", Version: "v1.0.0"})
	if err != nil {
		return err
	}
	if err := app.Register(foundation.Worker("heartbeat", func(ctx context.Context) error {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				logger.Info("worker heartbeat")
			}
		}
	})); err != nil {
		return err
	}
	return app.Run(ctx)
}

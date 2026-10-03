package health_test

import (
	"context"
	"errors"
	"testing"

	"github.com/thanhbvha/go-common/health"
)

func TestHealthChecks(t *testing.T) {
	c := health.New().WithVersion("v1.0.0")

	c.AddReadinessCheck("db", health.CustomChecker(func(ctx context.Context) error {
		return nil
	}))
	
	c.AddReadinessCheck("redis", health.CustomChecker(func(ctx context.Context) error {
		return errors.New("redis down")
	}))

	c.AddLivenessCheck("memory", health.CustomChecker(func(ctx context.Context) error {
		return nil
	}))

	res := c.CheckHealth(context.Background())
	if res.Status != health.StatusDown {
		t.Errorf("expected health down due to redis, got %v", res.Status)
	}

	if res.Version != "v1.0.0" {
		t.Errorf("expected version v1.0.0, got %v", res.Version)
	}

	liveRes := c.CheckLiveness(context.Background())
	if liveRes.Status != health.StatusOK {
		t.Errorf("expected liveness ok, got %v", liveRes.Status)
	}
}

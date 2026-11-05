package health_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/health"
)

func TestSingleCheckCancellationAcrossProbeTypes(t *testing.T) {
	for _, scope := range []string{"readiness", "liveness", "health"} {
		t.Run(scope, func(t *testing.T) {
			checker := health.New().WithVersion("v1").WithErrorDetails(true)
			var called atomic.Bool
			check := func(context.Context) error { called.Store(true); return nil }
			probe := checker.CheckReadiness
			switch scope {
			case "liveness":
				checker.AddLivenessCheck("dependency", check)
				probe = checker.CheckLiveness
			case "health":
				checker.AddReadinessCheck("dependency", check)
				probe = checker.CheckHealth
			default:
				checker.AddReadinessCheck("dependency", check)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			result := probe(ctx)
			if result.Status != health.StatusDown || result.Version != "v1" || len(result.Checks) != 1 || result.Checks["dependency"].Error != context.Canceled.Error() || called.Load() {
				t.Fatalf("canceled probe: %+v, callback called=%t", result, called.Load())
			}
			if result := probe(t.Context()); result.Status != health.StatusOK || !called.Load() {
				t.Fatalf("subsequent healthy probe: %+v", result)
			}
		})
	}
}

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

func TestHealthCheckTimeoutBoundsUncooperativeChecker(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	checker := health.New().WithTimeout(20 * time.Millisecond)
	checker.AddReadinessCheck("blocked", func(context.Context) error {
		<-release
		return nil
	})

	started := time.Now()
	result := checker.CheckReadiness(t.Context())
	if result.Status != health.StatusDown {
		t.Fatalf("expected timeout to mark readiness down, got %s", result.Status)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("health check was not bounded: %s", elapsed)
	}
}

func TestHealthCheckPanicIsSanitized(t *testing.T) {
	checker := health.New().WithErrorDetails(true)
	checker.AddReadinessCheck("panic", func(context.Context) error {
		panic("database-password")
	})

	result := checker.CheckReadiness(t.Context())
	check := result.Checks["panic"]
	if result.Status != health.StatusDown || check.Status != health.StatusDown {
		t.Fatalf("expected panic to mark readiness down: %+v", result)
	}
	if !strings.Contains(check.Error, health.ErrCheckPanic.Error()) {
		t.Fatalf("expected sanitized panic error, got %q", check.Error)
	}
	if strings.Contains(check.Error, "database-password") {
		t.Fatalf("panic value leaked through health response: %q", check.Error)
	}
}

func TestCheckerConfigurationIsConcurrentSafe(t *testing.T) {
	checker := health.New().AddReadinessCheck("ready", func(context.Context) error { return nil })
	var waitGroup sync.WaitGroup
	for index := range 50 {
		waitGroup.Add(2)
		go func() {
			defer waitGroup.Done()
			checker.WithVersion("v1").WithTimeout(time.Second).WithErrorDetails(index%2 == 0)
		}()
		go func() {
			defer waitGroup.Done()
			_ = checker.CheckReadiness(t.Context())
		}()
	}
	waitGroup.Wait()
}

func TestHealthChecksHideErrorDetailsByDefault(t *testing.T) {
	c := health.New().AddReadinessCheck("database", func(context.Context) error {
		return errors.New("postgres://user:secret@internal-db/service")
	})

	res := c.CheckReadiness(context.Background())
	if got := res.Checks["database"].Error; got != "" {
		t.Fatalf("expected dependency error to be hidden, got %q", got)
	}

	res = c.WithErrorDetails(true).CheckReadiness(context.Background())
	if got := res.Checks["database"].Error; got == "" {
		t.Fatal("expected dependency error when diagnostic details are enabled")
	}
}

func TestRepeatedProbesDoNotStartConcurrentBlockedChecks(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	checker := health.New().WithTimeout(time.Millisecond).AddReadinessCheck("blocked", func(context.Context) error { entered <- struct{}{}; <-release; return nil })
	if checker.CheckReadiness(t.Context()).Status != health.StatusDown {
		t.Fatal("blocked check healthy")
	}
	<-entered
	for range 100 {
		if checker.CheckReadiness(t.Context()).Status != health.StatusDown {
			t.Fatal("busy check healthy")
		}
	}
	select {
	case <-entered:
		t.Fatal("checker executed concurrently")
	default:
	}
}

// Package health provides HTTP health check endpoints for microservices.
//
// It implements three standard Kubernetes-ready endpoints:
//   - /health — overall health (all checks combined)
//   - /ready  — readiness: service is ready to accept traffic
//   - /live   — liveness: service process is alive
//
// Basic usage:
//
//	h := health.New().
//	    WithVersion("v1.2.0").
//	    AddReadinessCheck("database", health.GORMChecker(db)).
//	    AddReadinessCheck("redis", health.RedisChecker(redisClient))
//
//	app.Get("/health", fiberhealth.Handler(h))
//	app.Get("/ready",  fiberhealth.ReadyHandler(h))
//	app.Get("/live",   fiberhealth.LiveHandler(h))
package health

import (
	"context"
	"sync"
	"time"
)

// CheckFunc is a function that performs a single dependency health check.
// Return nil if healthy, non-nil error if unhealthy.
type CheckFunc func(ctx context.Context) error

// Status represents the health status of the service or a dependency.
type Status string

const (
	StatusOK   Status = "ok"
	StatusDown Status = "down"
)

// CheckResult is the result of a single named health check.
type CheckResult struct {
	Status     Status `json:"status"`
	Error      string `json:"error,omitempty"`
	DurationMs int64  `json:"duration_ms"`
}

// HealthResponse is the JSON body returned by health endpoints.
type HealthResponse struct {
	Status  Status                 `json:"status"`
	Checks  map[string]CheckResult `json:"checks,omitempty"`
	Version string                 `json:"version,omitempty"`
}

type checkEntry struct {
	name string
	fn   CheckFunc
}

// Checker manages a set of named health check functions.
// It is safe for concurrent use.
type Checker struct {
	mu              sync.RWMutex
	readinessChecks []checkEntry
	livenessChecks  []checkEntry
	version         string
	timeout         time.Duration
}

// New creates a new Checker with a default 5-second per-check timeout.
func New() *Checker {
	return &Checker{timeout: 5 * time.Second}
}

// WithVersion sets the service version included in every health response.
func (c *Checker) WithVersion(v string) *Checker {
	c.version = v
	return c
}

// WithTimeout sets the maximum duration for each individual check. Default: 5s.
func (c *Checker) WithTimeout(d time.Duration) *Checker {
	c.timeout = d
	return c
}

// AddReadinessCheck registers a check for the /ready endpoint.
// Readiness checks typically verify external dependencies (DB, cache, upstream APIs).
func (c *Checker) AddReadinessCheck(name string, fn CheckFunc) *Checker {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readinessChecks = append(c.readinessChecks, checkEntry{name, fn})
	return c
}

// AddLivenessCheck registers a check for the /live endpoint.
// Liveness checks should be lightweight (e.g., memory usage, goroutine count).
func (c *Checker) AddLivenessCheck(name string, fn CheckFunc) *Checker {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.livenessChecks = append(c.livenessChecks, checkEntry{name, fn})
	return c
}

// CheckReadiness runs all readiness checks concurrently.
func (c *Checker) CheckReadiness(ctx context.Context) HealthResponse {
	c.mu.RLock()
	checks := make([]checkEntry, len(c.readinessChecks))
	copy(checks, c.readinessChecks)
	c.mu.RUnlock()
	return c.run(ctx, checks)
}

// CheckLiveness runs all liveness checks concurrently.
func (c *Checker) CheckLiveness(ctx context.Context) HealthResponse {
	c.mu.RLock()
	checks := make([]checkEntry, len(c.livenessChecks))
	copy(checks, c.livenessChecks)
	c.mu.RUnlock()
	return c.run(ctx, checks)
}

// CheckHealth runs all checks (readiness + liveness) concurrently.
func (c *Checker) CheckHealth(ctx context.Context) HealthResponse {
	c.mu.RLock()
	all := append(append([]checkEntry{}, c.readinessChecks...), c.livenessChecks...)
	c.mu.RUnlock()
	return c.run(ctx, all)
}

func (c *Checker) run(ctx context.Context, checks []checkEntry) HealthResponse {
	if len(checks) == 0 {
		return HealthResponse{Status: StatusOK, Version: c.version}
	}

	type result struct {
		name string
		res  CheckResult
	}
	ch := make(chan result, len(checks))

	for _, e := range checks {
		go func(entry checkEntry) {
			checkCtx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()

			start := time.Now()
			err := entry.fn(checkCtx)
			durMs := time.Since(start).Milliseconds()

			r := CheckResult{Status: StatusOK, DurationMs: durMs}
			if err != nil {
				r.Status = StatusDown
				r.Error = err.Error()
			}
			ch <- result{name: entry.name, res: r}
		}(e)
	}

	results := make(map[string]CheckResult, len(checks))
	overall := StatusOK
	for range checks {
		r := <-ch
		results[r.name] = r.res
		if r.res.Status == StatusDown {
			overall = StatusDown
		}
	}

	return HealthResponse{Status: overall, Checks: results, Version: c.version}
}

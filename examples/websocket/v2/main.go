// Command v2 demonstrates authenticated, tenant-scoped WebSocket routing,
// EventBus dispatch and bounded shutdown with explicit dependency ownership.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type settings struct {
	address, namespace, delivery, business string
	redisURL, natsURL                      string
	roomNodeIndex                          bool
	registryConcurrency, dedupeCapacity    int
	leaseTTL, renewInterval, drainTimeout  time.Duration
	credentials                            []credential
	adminToken                             string
}

func defaults() settings {
	return settings{
		address: "127.0.0.1:8090", namespace: "ws-v2-demo", delivery: "redis", business: "redis",
		redisURL: "redis://127.0.0.1:16379/0", natsURL: "nats://127.0.0.1:14222",
		registryConcurrency: 32, dedupeCapacity: 100000, leaseTTL: 30 * time.Second,
		renewInterval: 10 * time.Second, drainTimeout: 5 * time.Second,
	}
}

func (s settings) validate() error {
	if s.delivery != "redis" && s.delivery != "nats" {
		return errors.New("delivery must be redis or nats")
	}
	if s.business != "redis" && s.business != "memory" {
		return errors.New("eventbus must be redis or memory")
	}
	if strings.TrimSpace(s.namespace) != s.namespace || s.namespace == "" || len(s.namespace) > 128 {
		return errors.New("namespace must contain 1–128 characters without surrounding whitespace")
	}
	if s.registryConcurrency < 1 || s.dedupeCapacity < 1 || s.leaseTTL < time.Millisecond || s.renewInterval <= 0 || s.renewInterval >= s.leaseTTL || s.drainTimeout <= 0 {
		return errors.New("invalid concurrency, cache or lifecycle settings")
	}
	if len(s.credentials) == 0 || len(s.adminToken) < 32 {
		return errors.New("configure client credentials and a separate admin credential of at least 32 bytes")
	}
	seen := map[string]bool{s.adminToken: true}
	for _, c := range s.credentials {
		if len(c.token) < 32 || seen[c.token] || c.identity.TenantID == "" || c.identity.UserID == "" || c.identity.SessionID == "" || c.identity.ExpiresAt.IsZero() {
			return errors.New("credentials must be unique, at least 32 bytes and bound to a complete server-side identity")
		}
		seen[c.token] = true
	}
	return nil
}

func main() {
	s := defaults()
	flag.StringVar(&s.address, "addr", s.address, "HTTP listen address")
	flag.StringVar(&s.namespace, "namespace", s.namespace, "Shared EventBus and routing namespace")
	flag.StringVar(&s.delivery, "delivery", s.delivery, "Targeted delivery: redis or nats")
	flag.StringVar(&s.business, "eventbus", s.business, "Business transport: redis or memory (process-local development)")
	flag.BoolVar(&s.roomNodeIndex, "room-node-index", false, "Enable indexed room reads after all namespace writers are upgraded")
	flag.IntVar(&s.registryConcurrency, "registry-concurrency", s.registryConcurrency, "Maximum concurrent registry operations")
	flag.IntVar(&s.dedupeCapacity, "dedupe-capacity", s.dedupeCapacity, "Gateway dedupe capacity across all cache stripes")
	flag.DurationVar(&s.leaseTTL, "lease-ttl", s.leaseTTL, "Session and room lease TTL")
	flag.DurationVar(&s.renewInterval, "renew-interval", s.renewInterval, "Lease renewal interval")
	flag.DurationVar(&s.drainTimeout, "drain-timeout", s.drainTimeout, "Outbound drain budget")
	flag.Parse()
	// Credentials/credential-bearing URLs are environment-only: flag help cannot
	// disclose them, and the program never logs their values.
	if value := os.Getenv("WS_V2_REDIS_URL"); value != "" {
		s.redisURL = value
	}
	if value := os.Getenv("WS_V2_NATS_URL"); value != "" {
		s.natsURL = value
	}
	s.credentials = environmentCredentials(time.Now().Add(time.Hour))
	s.adminToken = os.Getenv("WS_V2_ADMIN_TOKEN")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, s); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, s settings) (result error) {
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	app, err := newApplication(startup, s)
	cancel()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, app.close()) }()
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return errors.New("cannot open HTTP listener")
	}
	server := &http.Server{Handler: app.handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	log.Printf("WebSocket v2 ready on %s (delivery=%s, eventbus=%s)", listener.Addr(), s.delivery, s.business)
	select {
	case err = <-served:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("HTTP server stopped")
	case <-ctx.Done():
		app.stopping.Store(true) // Readiness and HTTP admission stop before business drain.
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = server.Shutdown(shutdown)
		cancel()
		if err != nil {
			_ = server.Close()
		}
		return err
	}
}

// Command production demonstrates an explicitly wired backend service. It never
// migrates a database on startup. TLS is normally terminated at a trusted proxy.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/v2/auth"
	authhttp "github.com/thanhbvha/go-common/v2/auth/middleware/nethttp"
	"github.com/thanhbvha/go-common/v2/config"
	"github.com/thanhbvha/go-common/v2/db/orm"
	"github.com/thanhbvha/go-common/v2/db/orm/backend"
	"github.com/thanhbvha/go-common/v2/db/orm/backend/postgres"
	"github.com/thanhbvha/go-common/v2/foundation"
	foundationhttp "github.com/thanhbvha/go-common/v2/foundation/adapter/nethttp"
	healthhttp "github.com/thanhbvha/go-common/v2/health/handler/nethttp"
	"github.com/thanhbvha/go-common/v2/logger"
	"github.com/thanhbvha/go-common/v2/ratelimit"
	limithttp "github.com/thanhbvha/go-common/v2/ratelimit/middleware/nethttp"
	commonredis "github.com/thanhbvha/go-common/v2/redis"
	"github.com/thanhbvha/go-common/v2/telemetry"
	"github.com/thanhbvha/go-common/v2/utils/ctxkey"
	webhttp "github.com/thanhbvha/go-common/v2/web/middleware/nethttp"
)

type settings struct {
	Address       string `mapstructure:"address"`
	PostgresDSN   string `mapstructure:"postgres_dsn"`
	RedisAddress  string `mapstructure:"redis_address"`
	RedisPassword string `mapstructure:"redis_password"`
	AuthSecret    string `mapstructure:"auth_secret"`
	OTLPEndpoint  string `mapstructure:"otlp_endpoint"`
	OTLPInsecure  bool   `mapstructure:"otlp_insecure"`
	RateLimit     int    `mapstructure:"rate_limit"`
}

func (c settings) Validate() error {
	if len(c.AuthSecret) < 32 {
		return errors.New("APP_AUTH_SECRET requires at least 32 random characters")
	}
	if c.PostgresDSN == "" {
		return errors.New("APP_POSTGRES_DSN is required")
	}
	if _, _, err := net.SplitHostPort(c.Address); err != nil {
		return errors.New("invalid APP_ADDRESS")
	}
	if _, _, err := net.SplitHostPort(c.RedisAddress); err != nil {
		return errors.New("invalid APP_REDIS_ADDRESS")
	}
	if c.RateLimit <= 0 {
		return errors.New("APP_RATE_LIMIT must be positive")
	}
	return nil
}

type limiterProxy struct{ value atomic.Pointer[limiterValue] }
type limiterValue struct{ limiter ratelimit.Limiter }

func (l *limiterProxy) Allow(ctx context.Context, key string, cfg ratelimit.Config) (*ratelimit.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	v := l.value.Load()
	if v == nil {
		return nil, errors.New("limiter not initialized")
	}
	return v.limiter.Allow(ctx, key, cfg)
}

type revocations struct{ client *commonredis.Client }

func (r revocations) IsRevoked(ctx context.Context, id string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	client := r.client.Native()
	if client == nil {
		return false, errors.New("redis not initialized")
	}
	n, err := client.Exists(ctx, "reference:revoked:"+id).Result()
	return n > 0, err
}

func authenticatedSubjectKey(r *http.Request) string {
	value, _ := ctxkey.GetUserInfo(r.Context())
	user, ok := value.(*auth.UserInfo)
	if !ok || user == nil || user.ID == "" {
		return ""
	}
	// Redis keys must not expose the subject or inherit its unbounded length.
	return fmt.Sprintf("%x", sha256.Sum256([]byte(user.ID)))
}

type service struct {
	app     *foundation.App
	handler http.Handler
	tokens  *auth.Manager
	server  *http.Server
}

func build(c settings, driver backend.Backend, writer io.Writer, options ...foundation.Option) (*service, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	redisCfg := commonredis.DefaultConfig()
	host, port, _ := net.SplitHostPort(c.RedisAddress)
	redisCfg.Host = host
	redisCfg.Port, _ = strconv.Atoi(port)
	redisCfg.Password = c.RedisPassword
	redisClient := commonredis.New(redisCfg)
	tokens, err := auth.NewManagerWithOptions(auth.ManagerOptions{SecretKey: c.AuthSecret, Issuer: "go-common-reference", Audience: []string{"reference-api"}, MaxLifetime: 2 * time.Hour, Revocation: revocations{redisClient}})
	if err != nil {
		return nil, err
	}
	logs := logger.New(logger.Options{Writer: writer})
	options = append(options, foundation.WithLogger(logs))
	app, err := foundation.New(foundation.Config{Name: "reference-api", Version: "v2", StartupTimeout: 15 * time.Second, ShutdownTimeout: 15 * time.Second}, options...)
	if err != nil {
		logs.Close()
		return nil, err
	}
	if err = app.Register(foundation.Resource("logger", nil, logs.Shutdown)); err != nil {
		return nil, err
	}
	var tel *telemetry.Telemetry
	if err = app.Register(foundation.Resource("telemetry", func(ctx context.Context) error {
		cfg := telemetry.DefaultConfig("reference-api")
		cfg.Endpoint = c.OTLPEndpoint
		cfg.Insecure = c.OTLPInsecure
		cfg.EnableMetrics = cfg.Endpoint != ""
		cfg.EnableTracing = cfg.Endpoint != ""
		var e error
		tel, e = telemetry.New(ctx, cfg)
		if e != nil {
			return e
		}
		return tel.InstallGlobal()
	}, func(ctx context.Context) error {
		if tel == nil {
			return nil
		}
		return tel.Shutdown(ctx)
	})); err != nil {
		return nil, err
	}
	var database atomic.Pointer[orm.Client]
	if err = app.Register(foundation.Resource("postgres", func(ctx context.Context) error {
		cfg := orm.DefaultConfig()
		cfg.Backend = driver
		cfg.Logger = logs.Logger()
		cfg.EnableTelemetry = true
		db, e := orm.Open(ctx, cfg)
		if e == nil {
			database.Store(db)
		}
		return e
	}, func(ctx context.Context) error {
		if db := database.Load(); db != nil {
			return db.Close(ctx)
		}
		return nil
	})); err != nil {
		return nil, err
	}
	app.Health().WithTimeout(500 * time.Millisecond)
	limiter := &limiterProxy{}
	if err = app.Register(foundation.Resource("redis", func(ctx context.Context) error {
		if e := redisClient.Connect(ctx); e != nil {
			return e
		}
		limit, e := ratelimit.NewRedisLimiter(redisClient.Native(), "reference:rate")
		if e != nil {
			return errors.Join(e, redisClient.Close())
		}
		limiter.value.Store(&limiterValue{limit})
		return nil
	}, func(context.Context) error { return redisClient.Close() })); err != nil {
		return nil, err
	}
	app.Health().AddReadinessCheck("postgres", func(ctx context.Context) error {
		db := database.Load()
		if db == nil {
			return errors.New("database not initialized")
		}
		return db.Ping(ctx)
	})
	app.Health().AddReadinessCheck("redis", redisClient.Ping)
	limitMiddleware, err := limithttp.Middleware(limiter, ratelimit.MiddlewareConfig{Policy: ratelimit.Config{Limit: c.RateLimit, Window: time.Second}, FailurePolicy: ratelimit.FailClosed, OnError: func(ctx context.Context, _ error) { logs.Logger().WarnContext(ctx, "rate limiter unavailable") }}, authenticatedSubjectKey)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("GET /live", healthhttp.LiveHandler(app.Health()))
	mux.Handle("GET /ready", healthhttp.ReadyHandler(app.Health()))
	me := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value, _ := ctxkey.GetUserInfo(r.Context())
		user, ok := value.(*auth.UserInfo)
		if !ok || user == nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"user_id": user.ID})
	})
	mux.Handle("GET /v1/me", authhttp.Middleware(tokens)(limitMiddleware(me)))
	handler := webhttp.Recover(webhttp.Telemetry("reference-api")(mux))
	server := &http.Server{Addr: c.Address, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 32 << 10}
	if err = app.Register(foundationhttp.Component("http", server)); err != nil {
		return nil, err
	}
	return &service{app: app, handler: handler, tokens: tokens, server: server}, nil
}
func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("production", flag.ContinueOnError)
	mint := flags.String("mint-token", "", "issue a short-lived token for a synthetic subject, then exit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	c := settings{Address: "127.0.0.1:8080", RedisAddress: "127.0.0.1:6379", RateLimit: 100}
	opts := config.DefaultOptions()
	opts.EnvPrefix = "APP"
	if err := config.Load(opts, &c); err != nil {
		return err
	}
	if err := c.Validate(); err != nil {
		return err
	}
	if *mint != "" {
		manager, err := auth.NewManagerWithOptions(auth.ManagerOptions{SecretKey: c.AuthSecret, Issuer: "go-common-reference", Audience: []string{"reference-api"}, MaxLifetime: 2 * time.Hour})
		if err != nil {
			return err
		}
		token, err := manager.GenerateTokenContext(ctx, auth.UserInfo{ID: *mint}, 15*time.Minute)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(os.Stdout, token)
		return err
	}
	service, err := build(c, postgres.New(c.PostgresDSN), os.Stdout)
	if err != nil {
		return err
	}
	return service.app.Run(ctx)
}
func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		slog.Error("reference service stopped", "reason", strings.Split(err.Error(), ":")[0])
		os.Exit(1)
	}
}

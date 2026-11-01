// Package demo contains the wiring shared by the standalone server and client.
package demo

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/telemetry"
)

const maxMessageBytes = 64 << 10

// CommonOptions are application settings, not library-wide defaults.
type CommonOptions struct {
	// Token may be supplied by a private YAML file; never log configuration values.
	Token        string `mapstructure:"token"`
	Insecure     bool   `mapstructure:"insecure"`
	Cert         string `mapstructure:"cert"`
	Key          string `mapstructure:"key"`
	CA           string `mapstructure:"ca"`
	ServerName   string `mapstructure:"server_name"`
	OTLPEndpoint string `mapstructure:"otlp_endpoint"`
	OTLPInsecure bool   `mapstructure:"otlp_insecure"`
}

func (o *CommonOptions) flags(fs *flag.FlagSet) {
	fs.BoolVar(&o.Insecure, "insecure", o.Insecure, "explicitly use plaintext on loopback only")
	fs.StringVar(&o.Cert, "cert", o.Cert, "PEM certificate (server required with TLS; client optional for mTLS)")
	fs.StringVar(&o.Key, "key", o.Key, "PEM private key paired with -cert")
	fs.StringVar(&o.CA, "ca", o.CA, "PEM CA bundle (server: require mTLS; client: trust private CA)")
	fs.StringVar(&o.ServerName, "server-name", o.ServerName, "client TLS server name override")
	fs.StringVar(&o.OTLPEndpoint, "otlp-endpoint", o.OTLPEndpoint, "enable OTLP traces/metrics, host:port; empty disables export")
	fs.BoolVar(&o.OTLPInsecure, "otlp-insecure", o.OTLPInsecure, "plaintext OTLP for a local collector")
}

// ServerOptions configures the example server.
type ServerOptions struct {
	CommonOptions      `mapstructure:",squash"`
	Address            string        `mapstructure:"addr"`
	Reflection         bool          `mapstructure:"reflection"`
	DemoErrors         bool          `mapstructure:"demo_errors"`
	ShutdownTimeout    time.Duration `mapstructure:"shutdown_timeout"`
	ReadyDelay         time.Duration `mapstructure:"ready_delay"`
	StartupTimeout     time.Duration `mapstructure:"startup_timeout"`
	AppShutdownTimeout time.Duration `mapstructure:"app_shutdown_timeout"`
}

func ParseServer(args []string) (ServerOptions, error) {
	o := ServerOptions{Address: "127.0.0.1:50051", ShutdownTimeout: 10 * time.Second, StartupTimeout: 30 * time.Second, AppShutdownTimeout: 30 * time.Second}
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	o.flags(fs)
	fs.StringVar(&o.Address, "addr", o.Address, "TCP listen address")
	fs.BoolVar(&o.Reflection, "reflection", o.Reflection, "enable authenticated server reflection")
	fs.BoolVar(&o.DemoErrors, "demo-errors", o.DemoErrors, "enable authenticated EchoError/EchoPanic demonstration endpoints")
	fs.DurationVar(&o.ShutdownTimeout, "shutdown-timeout", o.ShutdownTimeout, "maximum RPC drain time")
	fs.DurationVar(&o.ReadyDelay, "ready-delay", o.ReadyDelay, "simulate initialization while health reports NOT_SERVING")
	fs.DurationVar(&o.StartupTimeout, "startup-timeout", o.StartupTimeout, "foundation: total startup budget")
	fs.DurationVar(&o.AppShutdownTimeout, "app-shutdown-timeout", o.AppShutdownTimeout, "foundation: total shutdown budget (must exceed RPC drain timeout)")
	if err := parseConfiguration(fs, args, "GRPC_SERVER", &o); err != nil {
		return o, err
	}
	if o.ShutdownTimeout <= 0 || o.ReadyDelay < 0 {
		return o, errors.New("shutdown timeout must be positive and ready delay non-negative")
	}
	return o, nil
}

// ClientOptions configures independently selectable demonstration scenarios.
type ClientOptions struct {
	CommonOptions `mapstructure:",squash"`
	Target        string        `mapstructure:"target"`
	Scenario      string        `mapstructure:"scenario"`
	Message       string        `mapstructure:"message"`
	Timeout       time.Duration `mapstructure:"timeout"`
}

func ParseClient(args []string) (ClientOptions, error) {
	o := ClientOptions{Target: "localhost:50051", Scenario: "basic", Message: "Hello gRPC", Timeout: 3 * time.Second}
	fs := flag.NewFlagSet("client", flag.ContinueOnError)
	o.flags(fs)
	fs.StringVar(&o.Target, "target", o.Target, "gRPC target; resolver URIs supported with TLS")
	fs.StringVar(&o.Scenario, "scenario", o.Scenario, "basic|errors|deadline|health|watch")
	fs.StringVar(&o.Message, "message", o.Message, "message sent by basic scenario (not logged)")
	fs.DurationVar(&o.Timeout, "timeout", o.Timeout, "unary timeout and finite stream lifetime")
	if err := parseConfiguration(fs, args, "GRPC_CLIENT", &o); err != nil {
		return o, err
	}
	if o.Timeout <= 0 {
		return o, errors.New("timeout must be positive")
	}
	switch o.Scenario {
	case "basic", "errors", "deadline", "health", "watch":
	default:
		return o, errors.New("unknown scenario")
	}
	return o, nil
}

func sharedToken(token string) (string, error) {
	if override := os.Getenv("GRPC_DEMO_TOKEN"); override != "" {
		token = override
	}
	if len(token) < 32 {
		return "", errors.New("set YAML token or GRPC_DEMO_TOKEN to a shared random secret of at least 32 bytes")
	}
	for _, c := range []byte(token) {
		if c < 33 || c > 126 {
			return "", errors.New("gRPC token must contain printable ASCII without whitespace")
		}
	}
	return token, nil
}

func initTelemetry(ctx context.Context, service string, o CommonOptions) (func() error, error) {
	cleanup, err := initTelemetryProvider(ctx, service, o)
	if err != nil {
		return nil, err
	}
	return func() error {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return cleanup(shutdownCtx)
	}, nil
}

// The foundation variant uses its caller's cleanup context, keeping exporter
// flushing within the shared application shutdown budget.
func initTelemetryProvider(ctx context.Context, service string, o CommonOptions) (func(context.Context) error, error) {
	if o.OTLPEndpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	host, port, err := net.SplitHostPort(o.OTLPEndpoint)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || host == "" || strings.ContainsAny(host, "/@?#") || portErr != nil || portNumber < 1 || portNumber > 65535 {
		return nil, errors.New("-otlp-endpoint must be host:port without credentials or a URL scheme")
	}
	cfg := telemetry.DefaultConfig(service)
	cfg.Endpoint = o.OTLPEndpoint
	cfg.Insecure = o.OTLPInsecure
	cfg.SamplingRate = 0.1
	tel, err := telemetry.Init(ctx, cfg)
	if err != nil {
		return nil, errors.New("telemetry initialization failed")
	}
	return tel.Shutdown, nil
}

// Main runs a command with signals owned by its caller. Errors printed by these
// commands are intentionally sanitized: native network/TLS errors may contain paths.
func Main(ctx context.Context, mode string, args []string) int {
	l := logger.New(logger.Options{StdOut: true, NumWorkers: 1, BufferSize: 256})
	logger.SetDefault(l)
	defer func() { l.Close(); logger.SetDefault(nil) }()
	var err error
	switch mode {
	case "server", "foundation":
		var o ServerOptions
		o, err = ParseServer(args)
		if err == nil {
			if mode == "foundation" {
				err = RunFoundation(ctx, o)
			} else {
				err = RunServer(ctx, o)
			}
		}
	case "client":
		var o ClientOptions
		o, err = ParseClient(args)
		if err == nil {
			err = RunClient(ctx, o)
		}
	default:
		err = errors.New("unknown command")
	}
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

// combineCleanup preserves exporter/connection cleanup failures in the exit result.
func combineCleanup(result *error, cleanup func() error) {
	if err := cleanup(); err != nil {
		*result = errors.Join(*result, errors.New("resource cleanup failed"))
	}
}

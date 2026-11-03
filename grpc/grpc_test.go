package grpc_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	common "github.com/thanhbvha/go-common/v2/grpc"
	"github.com/thanhbvha/go-common/v2/xerrors"
	native "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func start(t *testing.T, cfg common.ServerConfig, clientCfg common.ClientConfig, opts ...native.ServerOption) (*common.Server, healthpb.HealthClient) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := common.NewServer(cfg, opts...)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.ServeContext(ctx, lis) }()
	clientCfg.Target = "passthrough:///bufnet"
	conn, err := common.NewClient(clientCfg, native.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("server failed to stop")
		}
	})
	return srv, healthpb.NewHealthClient(conn)
}

func TestTransportAndConfiguration(t *testing.T) {
	cfg := common.DefaultServerConfig
	cfg.EnableTelemetry = false
	srv, client := start(t, cfg, common.ClientConfig{Insecure: true})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	for name := range srv.GetServiceInfo() {
		if strings.Contains(name, "reflection") {
			t.Fatal("reflection enabled by default")
		}
	}
	srv.Health.SetServingStatus("app", healthpb.HealthCheckResponse_NOT_SERVING)
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: "app"})
	if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("readiness: %v %v", resp, err)
	}
	cfg.EnableReflection = true
	reflected := common.NewServer(cfg)
	defer reflected.Stop()
	if _, ok := reflected.GetServiceInfo()["grpc.reflection.v1.ServerReflection"]; !ok {
		t.Fatal("explicit reflection missing")
	}
	for _, invalid := range []common.ClientConfig{{}, {Target: "x", UnaryTimeout: -time.Second}, {Target: "x", Insecure: true, Credentials: credentials.NewTLS(&tls.Config{})}} {
		if conn, err := common.NewClient(invalid); err == nil {
			conn.Close()
			t.Fatal("accepted invalid config")
		}
	}
}

func TestTLS(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "bufnet"}, DNSNames: []string{"bufnet"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	cfg := common.ServerConfig{EnableHealth: true, Credentials: credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}})}
	_, client := start(t, cfg, common.ClientConfig{Credentials: credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "bufnet"})})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	// The default TLS client must reject a plaintext endpoint.
	cfg.Credentials = nil
	_, plain := start(t, cfg, common.ClientConfig{})
	short, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if _, err := plain.Check(short, &healthpb.HealthCheckRequest{}); err == nil {
		t.Fatal("TLS client accepted plaintext")
	}
}

func TestUnaryAndStreamFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure func() error
		code    codes.Code
	}{
		{"panic", func() error { panic("secret panic data") }, codes.Internal},
		{"unknown", func() error { return errors.New("secret database password") }, codes.Internal},
		{"domain", func() error { return xerrors.New("BAD", "invalid input", 422) }, codes.InvalidArgument},
		{"server", func() error { return xerrors.New("DB", "secret database password", 503) }, codes.Unavailable},
		{"canceled", func() error { return context.Canceled }, codes.Canceled},
		{"status", func() error { return status.Error(codes.PermissionDenied, "denied") }, codes.PermissionDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := common.ServerConfig{EnableRecovery: true, EnableLogger: true, EnableHealth: true}
			_, client := start(t, cfg, common.ClientConfig{Insecure: true},
				native.ChainUnaryInterceptor(func(context.Context, any, *native.UnaryServerInfo, native.UnaryHandler) (any, error) {
					return nil, tc.failure()
				}),
				native.ChainStreamInterceptor(func(any, native.ServerStream, *native.StreamServerInfo, native.StreamHandler) error {
					return tc.failure()
				}))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
			if status.Code(err) != tc.code || strings.Contains(err.Error(), "secret") {
				t.Fatalf("unary: %v", err)
			}
			stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
			if err == nil {
				_, err = stream.Recv()
			}
			if status.Code(err) != tc.code || strings.Contains(err.Error(), "secret") {
				t.Fatalf("stream: %v", err)
			}
		})
	}
}

func TestLimitsAndTimeout(t *testing.T) {
	cfg := common.ServerConfig{EnableHealth: true}
	_, client := start(t, cfg, common.ClientConfig{Insecure: true}, native.MaxRecvMsgSize(64))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: strings.Repeat("x", 128)})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("message limit: %v", err)
	}
	_, slow := start(t, cfg, common.ClientConfig{Insecure: true, UnaryTimeout: 50 * time.Millisecond}, native.ChainUnaryInterceptor(func(ctx context.Context, _ any, _ *native.UnaryServerInfo, _ native.UnaryHandler) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	_, err = slow.Check(ctx, &healthpb.HealthCheckRequest{})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline: %v", err)
	}
}

func TestShutdownDeadline(t *testing.T) {
	entered := make(chan struct{})
	cfg := common.ServerConfig{EnableHealth: true, ShutdownTimeout: 50 * time.Millisecond}
	srv, client := start(t, cfg, common.ClientConfig{Insecure: true}, native.ChainUnaryInterceptor(func(ctx context.Context, _ any, _ *native.UnaryServerInfo, _ native.UnaryHandler) (any, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); result <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("handler not entered")
	}
	if err := srv.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("RPC unexpectedly succeeded")
		}
	case <-ctx.Done():
		t.Fatal("RPC not canceled")
	}
}

func TestServeContextDrains(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	entered, release := make(chan struct{}), make(chan struct{})
	srv := common.NewServer(common.ServerConfig{EnableHealth: true, ShutdownTimeout: time.Second}, native.ChainUnaryInterceptor(func(ctx context.Context, req any, info *native.UnaryServerInfo, next native.UnaryHandler) (any, error) {
		close(entered)
		<-release
		return next(ctx, req)
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.ServeContext(ctx, lis) }()
	conn, err := common.NewClient(common.ClientConfig{Target: "passthrough:///bufnet", Insecure: true}, native.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rpcCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	rpcDone := make(chan error, 1)
	go func() {
		_, err := healthpb.NewHealthClient(conn).Check(rpcCtx, &healthpb.HealthCheckRequest{})
		rpcDone <- err
	}()
	select {
	case <-entered:
	case <-rpcCtx.Done():
		t.Fatal("handler not entered")
	}
	cancel()
	select {
	case err := <-done:
		close(release)
		t.Fatalf("returned before draining: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-rpcDone:
		if err != nil {
			t.Fatalf("active RPC failed: %v", err)
		}
	case <-rpcCtx.Done():
		t.Fatal("RPC did not complete")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-rpcCtx.Done():
		t.Fatal("server did not drain")
	}
}

func TestServeContextAlreadyCanceled(t *testing.T) {
	lis := bufconn.Listen(1024)
	srv := common.NewServer(common.ServerConfig{})
	defer srv.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := srv.ServeContext(ctx, lis); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if conn, err := lis.Dial(); err == nil {
		conn.Close()
		t.Fatal("listener remains open")
	}
}

func TestConcurrentShutdownRetainsForcedResult(t *testing.T) {
	entered := make(chan struct{})
	srv, client := start(t, common.ServerConfig{EnableHealth: true, ShutdownTimeout: 30 * time.Millisecond}, common.ClientConfig{Insecure: true}, native.ChainUnaryInterceptor(func(ctx context.Context, _ any, _ *native.UnaryServerInfo, _ native.UnaryHandler) (any, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rpcDone := make(chan error, 1)
	go func() { _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); rpcDone <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}
	results := make(chan error, 32)
	for i := 0; i < cap(results); i++ {
		go func() { results <- srv.Shutdown(ctx) }()
	}
	for i := 0; i < cap(results); i++ {
		select {
		case err := <-results:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("concurrent shutdown: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("shutdown blocked")
		}
	}
	if err := srv.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("forced result was lost: %v", err)
	}
	response, err := srv.Health.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || response.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("health after shutdown: %v %v", response, err)
	}
	select {
	case err := <-rpcDone:
		if err == nil {
			t.Fatal("RPC survived forced stop")
		}
	case <-ctx.Done():
		t.Fatal("transport was not closed")
	}
}

func TestExternalShutdownIsPartOfServeResult(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	entered, release := make(chan struct{}), make(chan struct{})
	// This handler deliberately ignores cancellation to exercise bounded Shutdown,
	// even when a native stop waits for handlers. Always release it at test exit.
	srv := common.NewServer(common.ServerConfig{EnableHealth: true, ShutdownTimeout: 30 * time.Millisecond}, native.WaitForHandlers(true), native.ChainUnaryInterceptor(func(context.Context, any, *native.UnaryServerInfo, native.UnaryHandler) (any, error) {
		close(entered)
		<-release
		return nil, nil
	}))
	defer srv.Stop()
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	serving := make(chan error, 1)
	go func() { serving <- srv.ServeContext(ctx, lis) }()
	conn, err := common.NewClient(common.ClientConfig{Target: "passthrough:///bufnet", Insecure: true}, native.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	go func() { _, _ = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- srv.Shutdown(ctx) }()
	select {
	case err := <-stopped:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("native wait escaped shutdown deadline")
	}
	select {
	case err := <-serving:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ServeContext lost external shutdown failure: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ServeContext did not return forced result")
	}
}

func TestListenAndServeBindFailureAndCancellation(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	srv := common.NewServer(common.ServerConfig{Address: occupied.Addr().String()})
	defer srv.Stop()
	if err := srv.ListenAndServe(context.Background()); err == nil {
		t.Fatal("occupied port was accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := srv.ListenAndServe(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled bind: %v", err)
	}
}

func TestShutdownInstancesAreIsolated(t *testing.T) {
	cfg := common.ServerConfig{EnableHealth: true}
	first, _ := start(t, cfg, common.ClientConfig{Insecure: true})
	_, otherClient := start(t, cfg, common.ClientConfig{Insecure: true})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := first.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Shutdown(ctx); err != nil {
		t.Fatalf("repeated graceful shutdown: %v", err)
	}
	if _, err := otherClient.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("one server shut down another instance: %v", err)
	}
}

func TestNewServerRequiresExplicitReadiness(t *testing.T) {
	srv := common.NewServer(common.ServerConfig{EnableHealth: true})
	defer srv.Stop()
	result, err := srv.Health.Check(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil || result.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("initial health=%v err=%v", result, err)
	}
}

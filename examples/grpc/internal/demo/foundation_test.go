package demo

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/thanhbvha/go-common/v2/examples/grpc/proto"
	"github.com/thanhbvha/go-common/v2/foundation"
	common "github.com/thanhbvha/go-common/v2/grpc"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func foundationOptions(t *testing.T) ServerOptions {
	t.Helper()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reserved.Addr().String()
	reserved.Close()
	return ServerOptions{CommonOptions: CommonOptions{Insecure: true, Token: rand.Text() + rand.Text()}, Address: address, ShutdownTimeout: 100 * time.Millisecond, StartupTimeout: time.Second, AppShutdownTimeout: 2 * time.Second}
}

func TestFoundationDrainOrder(t *testing.T) {
	for _, forced := range []bool{false, true} {
		name := "graceful"
		if forced {
			name = "forced"
		}
		t.Run(name, func(t *testing.T) {
			o := foundationOptions(t)
			if !forced {
				o.ShutdownTimeout = 2 * time.Second
				o.AppShutdownTimeout = 3 * time.Second
			}
			dependency := &exampleDependency{}
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			srv := common.NewServer(common.ServerConfig{EnableHealth: true, ShutdownTimeout: o.ShutdownTimeout}, grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
				if info.FullMethod == pb.EchoService_Echo_FullMethodName {
					close(entered)
					if forced {
						<-ctx.Done()
						return nil, ctx.Err()
					}
					<-release
				}
				return next(ctx, req)
			}))
			pb.RegisterEchoServiceServer(srv.Server, &echoService{})
			srv.Health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
			srv.Health.SetServingStatus(serviceName, healthpb.HealthCheckResponse_NOT_SERVING)
			app, err := newFoundationApp(o, srv, dependency, foundation.WithoutSignals())
			if err != nil {
				t.Fatal(err)
			}
			if dependency.ready.Load() {
				t.Fatal("dependency ready before startup")
			}
			initial, _ := srv.Health.Check(context.Background(), &healthpb.HealthCheckRequest{Service: serviceName})
			if initial.Status != healthpb.HealthCheckResponse_NOT_SERVING {
				t.Fatal("initial readiness incorrect")
			}
			appCtx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var runErr error
			go func() { runErr = app.Run(appCtx); close(done) }()
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				cancel()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("foundation cleanup stuck")
				}
				_ = srv.Shutdown(context.Background())
			})
			ctx, stop := context.WithTimeout(context.Background(), 4*time.Second)
			defer stop()
			for app.State() != foundation.StateRunning {
				select {
				case <-done:
					t.Fatalf("startup failed: %v", runErr)
				default:
				}
				if err := pause(ctx, time.Millisecond); err != nil {
					t.Fatal(err)
				}
			}
			if !dependency.ready.Load() {
				t.Fatal("gRPC started before dependency")
			}
			ready, _ := srv.Health.Check(ctx, &healthpb.HealthCheckRequest{Service: serviceName})
			if ready.Status != healthpb.HealthCheckResponse_SERVING {
				t.Fatal("readiness not enabled after startup")
			}
			conn, err := newClient(ClientOptions{CommonOptions: o.CommonOptions, Target: o.Address, Timeout: time.Second}, o.Token)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			rpc := make(chan error, 1)
			go func() {
				_, err := pb.NewEchoServiceClient(conn).Echo(ctx, &pb.EchoRequest{Message: "drain"})
				rpc <- err
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("RPC did not enter handler")
			}
			cancel()
			if !forced {
				for {
					health, _ := srv.Health.Check(ctx, &healthpb.HealthCheckRequest{Service: serviceName})
					if health.Status == healthpb.HealthCheckResponse_NOT_SERVING {
						break
					}
					if err := pause(ctx, time.Millisecond); err != nil {
						t.Fatal(err)
					}
				}
				if !dependency.ready.Load() {
					t.Fatal("dependency closed while RPC was still running")
				}
				select {
				case <-done:
					t.Fatal("application returned before draining active RPC")
				default:
				}
				releaseOnce.Do(func() { close(release) })
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("application shutdown exceeded budget")
			}
			if forced {
				if !errors.Is(runErr, context.DeadlineExceeded) {
					t.Fatalf("forced shutdown error lost: %v", runErr)
				}
			} else if runErr != nil {
				t.Fatal(runErr)
			}
			if dependency.ready.Load() {
				t.Fatal("dependency not closed after gRPC runner stopped")
			}
			select {
			case err := <-rpc:
				if forced && err == nil {
					t.Fatal("forced RPC succeeded")
				}
				if !forced && err != nil {
					t.Fatalf("graceful RPC failed: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("RPC did not terminate")
			}
		})
	}
}

func TestFoundationStartupRollback(t *testing.T) {
	o := foundationOptions(t)
	occupied, err := net.Listen("tcp", o.Address)
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	srv, err := newServer(o, o.Token)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())
	dependency := &exampleDependency{}
	app, err := newFoundationApp(o, srv, dependency, foundation.WithoutSignals())
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Run(context.Background()); err == nil {
		t.Fatal("bind failure was not reported")
	}
	if dependency.ready.Load() {
		t.Fatal("startup rollback did not close dependency")
	}
	health, _ := srv.Health.Check(context.Background(), &healthpb.HealthCheckRequest{Service: serviceName})
	if health.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatal("failed startup reported serving")
	}
}

func TestRunFoundationStartupDeadline(t *testing.T) {
	t.Setenv("GRPC_DEMO_TOKEN", "")
	o := foundationOptions(t)
	o.StartupTimeout = 20 * time.Millisecond
	o.ReadyDelay = time.Second
	if err := RunFoundation(context.Background(), o); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("startup deadline lost: %v", err)
	}
}

func TestFoundationConfiguration(t *testing.T) {
	t.Setenv("GRPC_SERVER_STARTUP_TIMEOUT", "3s")
	t.Setenv("GRPC_SERVER_APP_SHUTDOWN_TIMEOUT", "5s")
	o, err := ParseServer([]string{"-insecure", "-shutdown-timeout=1s"})
	if err != nil {
		t.Fatal(err)
	}
	if o.StartupTimeout != 3*time.Second || o.AppShutdownTimeout != 5*time.Second {
		t.Fatal("foundation environment options ignored")
	}
	for _, budgets := range []struct{ startup, drain, total time.Duration }{{0, time.Second, 2 * time.Second}, {time.Second, 0, time.Second}, {time.Second, time.Second, time.Second}} {
		invalid := o
		invalid.StartupTimeout = budgets.startup
		invalid.ShutdownTimeout = budgets.drain
		invalid.AppShutdownTimeout = budgets.total
		srv, err := newServer(invalid, rand.Text()+rand.Text())
		if err != nil {
			t.Fatal(err)
		}
		_, err = newFoundationApp(invalid, srv, &exampleDependency{}, foundation.WithoutSignals())
		srv.Stop()
		if err == nil {
			t.Fatal("invalid lifecycle budgets accepted")
		}
	}
}

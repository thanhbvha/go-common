package demo

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/examples/grpc/internal/devcert"
	pb "github.com/thanhbvha/go-common/examples/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestExampleScenarios(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "certs")
	if err := devcert.Generate(directory); err != nil {
		t.Fatal(err)
	}
	if err := devcert.Generate(directory); err == nil {
		t.Fatal("certificate directory was overwritten")
	}
	token := rand.Text() + rand.Text()
	for _, mode := range []string{"plaintext", "tls", "mtls"} {
		t.Run(mode, func(t *testing.T) {
			serverOpts := ServerOptions{Address: "127.0.0.1:50051", DemoErrors: true, Reflection: true, ShutdownTimeout: time.Second}
			clientOpts := ClientOptions{Target: "127.0.0.1:50051", Message: "test", Timeout: time.Second}
			if mode == "plaintext" {
				serverOpts.Insecure = true
				clientOpts.Insecure = true
			} else {
				serverOpts.Cert = filepath.Join(directory, "server.pem")
				serverOpts.Key = filepath.Join(directory, "server-key.pem")
				clientOpts.CA = filepath.Join(directory, "ca.pem")
				if mode == "mtls" {
					serverOpts.CA = clientOpts.CA
					clientOpts.Cert = filepath.Join(directory, "client.pem")
					clientOpts.Key = filepath.Join(directory, "client-key.pem")
				}
			}
			srv, err := newServer(serverOpts, token)
			if err != nil {
				t.Fatal(err)
			}
			srv.Health.SetServingStatus(serviceName, healthpb.HealthCheckResponse_SERVING)
			lis := bufconn.Listen(1 << 20)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- srv.ServeContext(ctx, lis) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(3 * time.Second):
					t.Error("server shutdown stuck")
				}
			})
			dialer := grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() })
			conn, err := newClient(clientOpts, token, dialer)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			for _, scenario := range []string{"basic", "errors", "health", "watch", "deadline"} {
				t.Run(scenario, func(t *testing.T) {
					o := clientOpts
					o.Scenario = scenario
					// Timeout is fixed at connection construction for unary calls. Watch owns its context.
					if scenario == "watch" {
						o.Timeout = 500 * time.Millisecond
					}
					if scenario == "deadline" {
						o.Timeout = 30 * time.Millisecond
						shortConn, err := newClient(o, token, dialer)
						if err != nil {
							t.Fatal(err)
						}
						defer shortConn.Close()
						if err = runScenario(ctx, shortConn, o); err != nil {
							t.Fatal(err)
						}
					} else if err := runScenario(ctx, conn, o); err != nil {
						t.Fatal(err)
					}
				})
			}
			badConn, err := newClient(clientOpts, rand.Text()+rand.Text(), dialer)
			if err != nil {
				t.Fatal(err)
			}
			defer badConn.Close()
			requestCtx, stop := context.WithTimeout(ctx, time.Second)
			defer stop()
			_, err = pb.NewEchoServiceClient(badConn).Echo(requestCtx, &pb.EchoRequest{Message: "denied"})
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("unary auth: %v", err)
			}
			stream, err := pb.NewEchoServiceClient(badConn).EchoStream(requestCtx, &pb.EchoRequest{Message: "denied"})
			if err == nil {
				_, err = stream.Recv()
			}
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("stream auth: %v", err)
			}
			_, err = healthpb.NewHealthClient(badConn).Check(requestCtx, &healthpb.HealthCheckRequest{})
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("health auth: %v", err)
			}
			if mode == "mtls" {
				noCert := clientOpts
				noCert.Cert = ""
				noCert.Key = ""
				rejected, err := newClient(noCert, token, dialer)
				if err != nil {
					t.Fatal(err)
				}
				defer rejected.Close()
				_, err = healthpb.NewHealthClient(rejected).Check(requestCtx, &healthpb.HealthCheckRequest{})
				if err == nil {
					t.Fatal("mTLS accepted client without certificate")
				}
			}
		})
	}
}

func TestConfigurationRejectsUnsafeCombinations(t *testing.T) {
	for _, address := range []string{":50051", "0.0.0.0:50051", "example.com:443", "dns:///localhost:50051"} {
		if _, err := transport(CommonOptions{Insecure: true}, address, false); err == nil {
			t.Fatalf("plaintext allowed on %s", address)
		}
	}
	for _, o := range []CommonOptions{{Cert: "only-cert"}, {Insecure: true, CA: "ca"}, {}} {
		if _, err := transport(o, "127.0.0.1:50051", true); err == nil {
			t.Fatalf("accepted invalid server transport: %+v", o)
		}
	}
	for _, args := range [][]string{{"-shutdown-timeout=0s"}, {"-ready-delay=-1s"}, {"unexpected"}} {
		if _, err := ParseServer(args); err == nil {
			t.Fatal("bad server flags accepted")
		}
	}
	for _, args := range [][]string{{"-scenario=unknown"}, {"-timeout=0s"}, {"unexpected"}} {
		if _, err := ParseClient(args); err == nil {
			t.Fatal("bad client flags accepted")
		}
	}
	t.Setenv("GRPC_DEMO_TOKEN", "")
	if _, err := sharedToken(""); err == nil {
		t.Fatal("empty token accepted")
	}
	t.Setenv("GRPC_DEMO_TOKEN", rand.Text()+"\n"+rand.Text())
	if _, err := sharedToken(""); err == nil {
		t.Fatal("metadata-unsafe token accepted")
	}
}

func TestRequestBoundaries(t *testing.T) {
	for _, req := range []*pb.EchoRequest{nil, {}, {Message: " "}, {Message: "valid", DelayMs: 5001}, {Message: "valid", Count: 11}} {
		if validate(req) == nil {
			t.Fatalf("invalid request accepted: %v", req)
		}
	}
	service := &echoService{}
	if _, err := service.EchoPanic(context.Background(), &pb.EchoRequest{Message: "disabled"}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("panic endpoint enabled by default")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pause(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatal("delay ignores cancellation")
	}
}

// Exercise the public server lifecycle with a real listener, readiness transition
// and cancellation; the other tests use in-memory transports for deterministic TLS.
func TestRunServerLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(context.Context, ServerOptions) error
	}{{"standalone", RunServer}, {"foundation", RunFoundation}} {
		t.Run(tc.name, func(t *testing.T) {

			token := rand.Text() + rand.Text()
			t.Setenv("GRPC_DEMO_TOKEN", "")
			reserved, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := reserved.Addr().String()
			reserved.Close()
			serverOptions, err := ParseServer([]string{"-config", configFile(t, "token: "+token+"\naddr: "+address+"\ninsecure: true\nready_delay: 100ms\nshutdown_timeout: 1s\n")})
			if err != nil {
				t.Fatal(err)
			}
			serverCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- tc.run(serverCtx, serverOptions)
			}()
			// Wait for the real listener before constructing a client. Otherwise an early
			// refused connection may enter gRPC's reconnect backoff and consume the test's
			// RPC budget before the first handshake, especially under the race detector.
			startupCtx, startupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer startupCancel()
			for {
				probe, dialErr := net.DialTimeout("tcp", address, 100*time.Millisecond)
				if dialErr == nil {
					_ = probe.Close()
					break
				}
				select {
				case err := <-done:
					t.Fatalf("server exited during startup: %v", err)
				default:
				}
				if err := pause(startupCtx, 10*time.Millisecond); err != nil {
					t.Fatal("server did not open its listener")
				}
			}
			t.Setenv("GRPC_CLIENT_TARGET", address)
			t.Setenv("GRPC_CLIENT_INSECURE", "true")
			t.Setenv("GRPC_CLIENT_TIMEOUT", "1s")
			clientOptions, err := ParseClient([]string{"-config", configFile(t, "token: "+token+"\nscenario: health\n")})
			if err != nil {
				t.Fatal(err)
			}
			conn, err := newClient(clientOptions, token)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			health := healthpb.NewHealthClient(conn)
			for {
				resp, err := health.Check(ctx, &healthpb.HealthCheckRequest{Service: serviceName}, grpc.WaitForReady(true))
				if err != nil {
					t.Fatal(err)
				}
				if resp.Status == healthpb.HealthCheckResponse_SERVING {
					break
				}
				if err = pause(ctx, 10*time.Millisecond); err != nil {
					t.Fatal(err)
				}
			}
			if err := RunClient(ctx, clientOptions); err != nil {
				t.Fatal(err)
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("RunServer did not drain and exit")
			}

		})
	}
}

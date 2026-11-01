package demo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	pb "github.com/thanhbvha/go-common/examples/grpc/proto"
	common "github.com/thanhbvha/go-common/grpc"
	"github.com/thanhbvha/go-common/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func newClient(o ClientOptions, token string, extra ...grpc.DialOption) (*grpc.ClientConn, error) {
	creds, err := transport(o.CommonOptions, o.Target, false)
	if err != nil {
		return nil, err
	}
	var auth credentials.PerRPCCredentials = bearerToken(token)
	if o.Insecure {
		auth = localBearerToken{bearerToken(token)}
	}
	opts := []grpc.DialOption{
		grpc.WithPerRPCCredentials(auth),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageBytes), grpc.MaxCallSendMsgSize(maxMessageBytes)),
	}
	return common.NewClient(common.ClientConfig{
		Target: o.Target, Credentials: creds, Insecure: o.Insecure,
		EnableTelemetry: o.OTLPEndpoint != "", UnaryTimeout: o.Timeout,
	}, append(opts, extra...)...)
}

// RunClient reuses one connection for unary, streaming and health RPCs. NewClient
// is lazy: the first RPC (not the constructor) verifies connectivity and TLS.
func RunClient(ctx context.Context, o ClientOptions) (result error) {
	token, err := sharedToken(o.Token)
	if err != nil {
		return err
	}
	if o.Scenario == "deadline" && o.Timeout >= 5*time.Second {
		return errors.New("deadline scenario requires -timeout below 5s")
	}
	cleanup, err := initTelemetry(ctx, "grpc-example-client", o.CommonOptions)
	if err != nil {
		return err
	}
	defer combineCleanup(&result, cleanup)
	conn, err := newClient(o, token)
	if err != nil {
		return err
	}
	defer combineCleanup(&result, conn.Close)
	return runScenario(ctx, conn, o)
}

func runScenario(ctx context.Context, conn *grpc.ClientConn, o ClientOptions) error {
	client := pb.NewEchoServiceClient(conn)
	switch o.Scenario {
	case "basic":
		resp, err := client.Echo(ctx, &pb.EchoRequest{Message: o.Message})
		if err != nil {
			return rpcFailure("Echo", err)
		}
		// Log size only: user messages can contain personal or sensitive data.
		logger.Info("Echo succeeded", "response_bytes", len(resp.Message))
		streamCtx, cancel := context.WithTimeout(ctx, o.Timeout)
		defer cancel()
		stream, err := client.EchoStream(streamCtx, &pb.EchoRequest{Message: o.Message, Count: 3, DelayMs: 100})
		if err != nil {
			return rpcFailure("EchoStream", err)
		}
		count := 0
		for {
			_, err = stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return rpcFailure("EchoStream.Recv", err)
			}
			count++
		}
		if count != 3 {
			return errors.New("EchoStream returned an unexpected message count")
		}
		logger.Info("EchoStream completed", "messages", count)
	case "errors":
		_, err := client.Echo(ctx, &pb.EchoRequest{})
		if err = expectCode("validation", err, codes.InvalidArgument); err != nil {
			return err
		}
		_, err = client.EchoError(ctx, &pb.EchoRequest{Message: "demo"})
		if err = expectCode("business error", err, codes.InvalidArgument); err != nil {
			return err
		}
		_, err = client.EchoPanic(ctx, &pb.EchoRequest{Message: "demo"})
		if err = expectCode("panic recovery", err, codes.Internal); err != nil {
			return err
		}
		// Prove the server remains available after a handler panic.
		if _, err = client.Echo(ctx, &pb.EchoRequest{Message: "still available"}); err != nil {
			return rpcFailure("post-panic Echo", err)
		}
		streamCtx, cancel := context.WithTimeout(ctx, o.Timeout)
		defer cancel()
		stream, err := client.EchoStream(streamCtx, &pb.EchoRequest{})
		if err == nil {
			_, err = stream.Recv()
		}
		return expectCode("stream validation", err, codes.InvalidArgument)
	case "deadline":
		// No per-call deadline here: ClientConfig.UnaryTimeout caps the slow handler.
		_, err := client.Echo(ctx, &pb.EchoRequest{Message: "slow", DelayMs: 5000})
		if err = expectCode("configured unary timeout", err, codes.DeadlineExceeded); err != nil {
			return err
		}
		shorter, cancel := context.WithTimeout(ctx, o.Timeout/2)
		defer cancel()
		_, err = client.Echo(shorter, &pb.EchoRequest{Message: "slow", DelayMs: 5000})
		return expectCode("shorter caller deadline", err, codes.DeadlineExceeded)
	case "health":
		resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: serviceName})
		if err != nil {
			return rpcFailure("Health.Check", err)
		}
		logger.Info("Health.Check", "status", resp.Status.String())
		if resp.Status != healthpb.HealthCheckResponse_SERVING {
			return errors.New("service is not ready")
		}
	case "watch":
		watchCtx, cancel := context.WithTimeout(ctx, o.Timeout)
		defer cancel()
		stream, err := healthpb.NewHealthClient(conn).Watch(watchCtx, &healthpb.HealthCheckRequest{Service: serviceName})
		if err != nil {
			return rpcFailure("Health.Watch", err)
		}
		updates := 0
		deadline, _ := watchCtx.Deadline()
		for {
			resp, err := stream.Recv()
			if err != nil {
				// Watch is intentionally infinite; this scenario owns and cancels its lifetime.
				// gRPC may report deadline expiry just before ctx.Err becomes
				// observable on this goroutine. Check the actual deadline too.
				expired := status.Code(err) == codes.DeadlineExceeded && !time.Now().Before(deadline)
				if updates > 0 && (watchCtx.Err() != nil || expired) {
					logger.Info("Health.Watch lifetime ended", "updates", updates)
					return nil
				}
				return rpcFailure("Health.Watch.Recv", err)
			}
			updates++
			logger.Info("Health.Watch update", "status", resp.Status.String())
		}
	}
	return nil
}

func rpcFailure(operation string, err error) error {
	return fmt.Errorf("%s failed: %s", operation, status.Code(err))
}
func expectCode(operation string, err error, want codes.Code) error {
	if status.Code(err) != want {
		return fmt.Errorf("%s: expected %s, got %s (check server -demo-errors)", operation, want, status.Code(err))
	}
	logger.Info("Expected RPC failure verified", "scenario", operation, "code", want.String())
	return nil
}

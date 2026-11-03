package demo

import (
	"context"
	"strings"
	"time"

	pb "github.com/thanhbvha/go-common/v2/examples/grpc/proto"
	"github.com/thanhbvha/go-common/v2/web/validator"
	"github.com/thanhbvha/go-common/v2/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const serviceName = "echo.EchoService"

type echoService struct {
	pb.UnimplementedEchoServiceServer
	demoErrors bool
}

func validate(req *pb.EchoRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "Request required")
	}
	input := struct {
		Message string `validate:"required,max=1024"`
		Delay   uint32 `validate:"lte=5000"`
		Count   uint32 `validate:"lte=10"`
	}{strings.TrimSpace(req.GetMessage()), req.GetDelayMs(), req.GetCount()}
	violations, err := validator.Validate(input)
	if err != nil {
		return err
	}
	if len(violations) != 0 {
		return xerrors.New("INVALID_REQUEST", "message must contain 1..1024 characters, delay_ms <= 5000, count <= 10", 422)
	}
	return nil
}

func pause(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *echoService) Echo(ctx context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	if err := validate(req); err != nil {
		return nil, err
	}
	if err := pause(ctx, time.Duration(req.DelayMs)*time.Millisecond); err != nil {
		return nil, err
	}
	return &pb.EchoResponse{Message: "Server says: " + req.Message}, nil
}

func (s *echoService) EchoError(ctx context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	if !s.demoErrors {
		return nil, status.Error(codes.PermissionDenied, "Demonstration endpoint disabled")
	}
	if err := validate(req); err != nil {
		return nil, err
	}
	return nil, xerrors.New("DEMO_INVALID", "Demonstration business validation error", 400)
}

func (s *echoService) EchoPanic(ctx context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	if !s.demoErrors {
		return nil, status.Error(codes.PermissionDenied, "Demonstration endpoint disabled")
	}
	if err := validate(req); err != nil {
		return nil, err
	}
	panic("demonstration handler panic")
}

func (s *echoService) EchoStream(req *pb.EchoRequest, stream grpc.ServerStreamingServer[pb.EchoResponse]) error {
	if err := validate(req); err != nil {
		return err
	}
	count := req.Count
	if count == 0 {
		count = 3
	}
	for i := uint32(0); i < count; i++ {
		if err := pause(stream.Context(), time.Duration(req.DelayMs)*time.Millisecond); err != nil {
			return err
		}
		if err := stream.Send(&pb.EchoResponse{Message: "Stream says: " + req.Message}); err != nil {
			return err
		}
	}
	return nil
}

package interceptors

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/thanhbvha/go-common/xerrors"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		http int
		code codes.Code
	}{
		{200, codes.Internal}, {201, codes.Internal}, {400, codes.InvalidArgument},
		{401, codes.Unauthenticated}, {403, codes.PermissionDenied}, {404, codes.NotFound},
		{408, codes.DeadlineExceeded}, {409, codes.AlreadyExists}, {413, codes.ResourceExhausted},
		{422, codes.InvalidArgument}, {429, codes.ResourceExhausted}, {500, codes.Internal},
		{501, codes.Unimplemented}, {503, codes.Unavailable}, {504, codes.DeadlineExceeded},
	} {
		t.Run(fmt.Sprint(tc.http), func(t *testing.T) {
			err := toStatus(fmt.Errorf("wrapped: %w", xerrors.Wrap(errors.New("private cause"), "CODE", "public message", tc.http)))
			if status.Code(err) != tc.code {
				t.Fatalf("got %v", err)
			}
			want := "public message"
			if tc.http >= 500 || tc.code == codes.Internal {
				want = "Internal server error"
			}
			if status.Convert(err).Message() != want {
				t.Fatalf("message: %v", err)
			}
		})
	}
	if toStatus(nil) != nil {
		t.Fatal("nil error changed")
	}
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{{context.Canceled, codes.Canceled}, {context.DeadlineExceeded, codes.DeadlineExceeded}, {errors.New("private"), codes.Internal}} {
		if got := status.Code(toStatus(fmt.Errorf("wrapped: %w", tc.err))); got != tc.code {
			t.Fatalf("got %v want %v", got, tc.code)
		}
	}
	st, err := status.New(codes.InvalidArgument, "public validation error").WithDetails(&errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: "name", Description: "required"}}})
	if err != nil {
		t.Fatal(err)
	}
	original := st.Err()
	if toStatus(original) != original {
		t.Fatal("explicit status/details were replaced")
	}
	if len(status.Convert(toStatus(original)).Details()) != 1 {
		t.Fatal("status details lost")
	}
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/examples/websocket/internal/exampleconfig"
	"github.com/thanhbvha/go-common/v2/websocket/core"
	"github.com/thanhbvha/go-common/v2/xerrors"
)

func TestConfiguredLimitsAndShutdownError(t *testing.T) {
	settings := exampleconfig.Default()
	settings.ExecutorPartitions = 2
	settings.MaxConnectionsPerIP = 2
	app, err := newApplication("standalone", "", "", "demo", "test", "admin", settings)
	if err != nil {
		t.Fatal(err)
	}
	defer app.close()
	_, connections := app.adapterLimiters()
	if !connections.AddConnection("peer") || connections.AddConnection("peer") {
		t.Fatal("configured per-IP connection limit not applied")
	}
	if app.manager.GetConfig().ExecutorPartitions != 2 {
		t.Fatal("configured executor partitions not applied")
	}
	err = app.shutdown(func(ctx context.Context) error {
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) <= 0 {
			t.Fatal("missing HTTP shutdown budget")
		}
		return context.DeadlineExceeded
	})
	if !errors.Is(err, context.DeadlineExceeded) || app.manager.State() != core.StateStopped {
		t.Fatalf("shutdown error=%v state=%v", err, app.manager.State())
	}
}

func TestAuthenticateDemo(t *testing.T) {
	tests := []struct {
		name          string
		expectedToken string
		userID        string
		authorization string
		wantUserID    string
		wantErr       bool
	}{
		{
			name:          "missing configured token",
			userID:        "user-1",
			authorization: "Bearer secret",
			wantErr:       true,
		},
		{
			name:          "missing bearer token",
			expectedToken: "secret",
			userID:        "user-1",
			wantErr:       true,
		},
		{
			name:          "invalid bearer token",
			expectedToken: "secret",
			userID:        "user-1",
			authorization: "Bearer wrong",
			wantErr:       true,
		},
		{
			name:          "missing user id",
			expectedToken: "secret",
			authorization: "Bearer secret",
			wantErr:       true,
		},
		{
			name:          "valid credentials",
			expectedToken: "secret",
			userID:        "user-1",
			authorization: "bearer secret",
			wantUserID:    "user-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("WS_DEMO_TOKEN", tt.expectedToken)

			userID, err := authenticateDemo(tt.userID, tt.authorization)
			if (err != nil) != tt.wantErr {
				t.Fatalf("authenticateDemo() error = %v, wantErr %v", err, tt.wantErr)
			}
			if userID != tt.wantUserID {
				t.Fatalf("authenticateDemo() userID = %q, want %q", userID, tt.wantUserID)
			}
		})
	}
}

type capturedErrorSender struct {
	encoded []byte
	accept  bool
}

func (s *capturedErrorSender) GetRequestID() string { return "request-error-test" }
func (s *capturedErrorSender) SendJSON(value any) bool {
	s.encoded, _ = json.Marshal(value)
	return s.accept
}

func TestSendErrorKeepsDiagnosticsOffWire(t *testing.T) {
	cause := errors.New("database password=super-secret-token")
	sender := &capturedErrorSender{accept: true}
	_ = sendError(sender, eventRoomJoin, cause)
	if strings.Contains(string(sender.encoded), "super-secret-token") {
		t.Fatalf("diagnostic cause leaked to client: %s", sender.encoded)
	}
	var frame struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Data      struct {
			Source  string `json:"source"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"data"`
	}
	if err := json.Unmarshal(sender.encoded, &frame); err != nil {
		t.Fatal(err)
	}
	if frame.Type != "error" || frame.RequestID != "request-error-test" || frame.Data.Source != eventRoomJoin || frame.Data.Code != "INTERNAL_ERROR" || frame.Data.Message != "An unexpected internal error occurred" {
		t.Fatalf("unexpected public response: %s", sender.encoded)
	}
}

func TestSendErrorPublicPolicyAndDiagnosticCause(t *testing.T) {
	secret := errors.New("upstream credential=super-secret-token")
	for _, tc := range []struct {
		name          string
		err           error
		code, message string
	}{
		{"unknown", secret, "INTERNAL_ERROR", "An unexpected internal error occurred"},
		{"validation", xerrors.Wrap(secret, "INVALID_ROOM", "Choose an available room", 400), "INVALID_ROOM", "Choose an available room"},
		{"internal structured", xerrors.Wrap(secret, "DB_CREDENTIAL_SECRET", "private database failure", 500), "INTERNAL_ERROR", "An unexpected internal error occurred"},
		{"wrapped authorization", fmt.Errorf("private upstream detail: %w", core.ErrUnauthorized), "FORBIDDEN", "You do not have permission to perform this operation"},
		{"room limit", core.ErrRoomLimit, "ROOM_LIMIT", "Room subscription limit reached"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sender := &capturedErrorSender{accept: true}
			err := sendError(sender, eventRoomJoin, tc.err)
			if !errors.Is(err, tc.err) {
				t.Fatalf("diagnostic cause lost: %v", err)
			}
			var frame struct {
				Data eventErrorPayload `json:"data"`
			}
			if err = json.Unmarshal(sender.encoded, &frame); err != nil {
				t.Fatal(err)
			}
			if frame.Data.Code != tc.code || frame.Data.Message != tc.message || strings.Contains(string(sender.encoded), "super-secret-token") {
				t.Fatalf("public response: %s", sender.encoded)
			}
		})
	}
	sender := &capturedErrorSender{accept: false}
	if err := sendError(sender, eventRoomJoin, secret); !errors.Is(err, secret) {
		t.Fatalf("queue rejection lost cause: %v", err)
	}
}

func TestDecodePayloadUsesPublicValidationMessages(t *testing.T) {
	for _, data := range []string{"", `{"room_id":{}}`} {
		var payload roomPayload
		err := decodePayload([]byte(data), &payload)
		frame := publicEventError(eventRoomJoin, err, "request")
		public := frame.Data.(eventErrorPayload)
		if public.Code == "INTERNAL_ERROR" || public.Message == err.Error() {
			t.Fatalf("validation policy missing: error=%v public=%+v", err, public)
		}
	}
}

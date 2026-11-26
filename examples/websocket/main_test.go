package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/thanhbvha/go-common/v2/websocket/core"
	"github.com/thanhbvha/go-common/v2/xerrors"
)

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

func TestRedisOptions(t *testing.T) {
	o, err := redisOptions("127.0.0.1:6379", "", "")
	if err != nil || o.Addr != "127.0.0.1:6379" || o.Password != "" {
		t.Fatalf("plain address: %+v %v", o, err)
	}
	o, err = redisOptions("redis://default:s3cret@10.0.0.5:6380/2", "", "")
	if err != nil || o.Addr != "10.0.0.5:6380" || o.Username != "default" || o.Password != "s3cret" || o.DB != 2 {
		t.Fatalf("url: %+v %v", o, err)
	}
	o, err = redisOptions("10.0.0.5:6379", "app", "pw")
	if err != nil || o.Username != "app" || o.Password != "pw" {
		t.Fatalf("separate credentials: %+v %v", o, err)
	}
	if _, err = redisOptions("redis://bad host:1", "", ""); err == nil || strings.Contains(err.Error(), "bad host") {
		t.Fatalf("invalid URL must fail without echoing it: %v", err)
	}
}

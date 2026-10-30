package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/examples/websocket/internal/exampleconfig"
	"github.com/thanhbvha/go-common/websocket/core"
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
	if !connections.AddConnection("peer") || !connections.AddConnection("peer") || connections.AddConnection("peer") {
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

package main

import "testing"

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

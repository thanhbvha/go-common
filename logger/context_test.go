package logger

import (
	"context"
	"testing"

	"github.com/thanhbvha/go-common/utils/ctxkey"
)

func TestExtractRequestIDSupportsSharedAndLegacyKeys(t *testing.T) {
	shared := ctxkey.SetRequestID(context.Background(), "shared")
	if got, ok := extractRequestID(shared); !ok || got != "shared" {
		t.Fatalf("shared request ID = %q, %v", got, ok)
	}

	legacy := context.WithValue(context.Background(), ContextKeyRequestID, "legacy")
	if got, ok := extractRequestID(legacy); !ok || got != "legacy" {
		t.Fatalf("legacy request ID = %q, %v", got, ok)
	}

	both := context.WithValue(shared, ContextKeyRequestID, "legacy")
	if got, ok := extractRequestID(both); !ok || got != "shared" {
		t.Fatalf("request ID precedence = %q, %v", got, ok)
	}
}

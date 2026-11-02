package ctxkey

import (
	"context"
	"testing"
)

func TestRequestIDRoundTrip(t *testing.T) {
	ctx := SetRequestID(context.Background(), "request-123")
	got, ok := GetRequestID(ctx)
	if !ok || got != "request-123" {
		t.Fatalf("GetRequestID() = %q, %v", got, ok)
	}
}

func TestUserInfoRoundTrip(t *testing.T) {
	want := struct{ ID string }{ID: "user-123"}
	ctx := SetUserInfo(context.Background(), want)
	got, ok := GetUserInfo(ctx)
	if !ok || got != want {
		t.Fatalf("GetUserInfo() = %#v, %v", got, ok)
	}
}

func TestAuthClaimsRoundTrip(t *testing.T) {
	want := struct{ ID string }{ID: "token-123"}
	ctx := SetAuthClaims(context.Background(), want)
	got, ok := GetAuthClaims(ctx)
	if !ok || got != want {
		t.Fatalf("GetAuthClaims() = %#v, %v", got, ok)
	}
}

func TestStringContextKeysDoNotCollide(t *testing.T) {
	type externalKey string
	ctx := context.WithValue(context.Background(), externalKey("request_id"), "external")
	ctx = SetRequestID(ctx, "internal")

	if got, _ := GetRequestID(ctx); got != "internal" {
		t.Fatalf("GetRequestID() = %q", got)
	}
	if got := ctx.Value(externalKey("request_id")); got != "external" {
		t.Fatalf("external context value = %v", got)
	}
}

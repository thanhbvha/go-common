// Package ctxkey provides a strongly-typed mechanism for managing context keys.
//
// It prevents key collisions when storing values in context.Context by using
// custom unexported types for keys, which is the recommended Go practice.
package ctxkey

import (
	"context"
)

// Key defines a custom type for context keys to avoid collisions
type Key string

const (
	// UserIDKey is the context key for the user ID
	UserIDKey Key = "user_id"
	
	// ClientIPKey is the context key for the client IP address
	ClientIPKey Key = "client_ip"
	
	// RequestIDKey is the context key for the unique request ID
	RequestIDKey Key = "request_id"
)

// SetUserID stores the user ID in the context
func SetUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, UserIDKey, userID)
}

// GetUserID retrieves the user ID from the context safely
func GetUserID(ctx context.Context) (string, bool) {
	val, ok := ctx.Value(UserIDKey).(string)
	return val, ok
}

// SetClientIP stores the client IP in the context
func SetClientIP(ctx context.Context, ip string) context.Context {
	return context.WithValue(ctx, ClientIPKey, ip)
}

// GetClientIP retrieves the client IP from the context safely
func GetClientIP(ctx context.Context) (string, bool) {
	val, ok := ctx.Value(ClientIPKey).(string)
	return val, ok
}

// SetRequestID stores the request ID in the context
func SetRequestID(ctx context.Context, reqID string) context.Context {
	return context.WithValue(ctx, RequestIDKey, reqID)
}

// GetRequestID retrieves the request ID from the context safely
func GetRequestID(ctx context.Context) (string, bool) {
	val, ok := ctx.Value(RequestIDKey).(string)
	return val, ok
}

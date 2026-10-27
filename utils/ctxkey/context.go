// Package ctxkey provides a strongly-typed mechanism for managing context keys.
//
// Accessor functions provide one consistent location for request metadata.
// The exported legacy keys remain available for framework-local compatibility.
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

	// UserInfo is the context key for the authenticated user information
	UserInfo Key = "user_info"

	// AuthClaims is the context key for validated registered JWT claims.
	AuthClaims Key = "auth_claims"

	// DataLoaderKey is the context key for GraphQL DataLoader
	DataLoaderKey Key = "data_loader"
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

// SetUserInfo stores authenticated user information in the context.
func SetUserInfo(ctx context.Context, userInfo any) context.Context {
	return context.WithValue(ctx, UserInfo, userInfo)
}

// GetUserInfo retrieves authenticated user information from the context.
func GetUserInfo(ctx context.Context) (any, bool) {
	val := ctx.Value(UserInfo)
	return val, val != nil
}

// SetAuthClaims stores validated JWT claims in the context.
func SetAuthClaims(ctx context.Context, claims any) context.Context {
	return context.WithValue(ctx, AuthClaims, claims)
}

// GetAuthClaims retrieves validated JWT claims from the context.
func GetAuthClaims(ctx context.Context) (any, bool) {
	val := ctx.Value(AuthClaims)
	return val, val != nil
}

// SetDataLoader stores any DataLoader in the context
func SetDataLoader(ctx context.Context, loader interface{}) context.Context {
	return context.WithValue(ctx, DataLoaderKey, loader)
}

// GetDataLoader retrieves the DataLoader from the context safely
func GetDataLoader(ctx context.Context) (interface{}, bool) {
	val := ctx.Value(DataLoaderKey)
	return val, val != nil
}

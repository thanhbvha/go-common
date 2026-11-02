// Package ctxkey provides collision-resistant accessors for request metadata.
package ctxkey

import "context"

// Framework local names are strings because Gin, Echo, and Fiber require string
// keys for their request-local stores. context.Context uses separate private keys.
const (
	LocalUserInfo   = "user_info"
	LocalAuthClaims = "auth_claims"
)

type key uint8

const (
	userIDKey key = iota
	clientIPKey
	requestIDKey
	userInfoKey
	authClaimsKey
	dataLoaderKey
)

func SetUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
}

func GetUserID(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(userIDKey).(string)
	return value, ok
}

func SetClientIP(ctx context.Context, clientIP string) context.Context {
	return context.WithValue(ctx, clientIPKey, clientIP)
}

func GetClientIP(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(clientIPKey).(string)
	return value, ok
}

func SetRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

func GetRequestID(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(requestIDKey).(string)
	return value, ok
}

func SetUserInfo(ctx context.Context, userInfo any) context.Context {
	return context.WithValue(ctx, userInfoKey, userInfo)
}

func GetUserInfo(ctx context.Context) (any, bool) {
	value := ctx.Value(userInfoKey)
	return value, value != nil
}

func SetAuthClaims(ctx context.Context, claims any) context.Context {
	return context.WithValue(ctx, authClaimsKey, claims)
}

func GetAuthClaims(ctx context.Context) (any, bool) {
	value := ctx.Value(authClaimsKey)
	return value, value != nil
}

func SetDataLoader(ctx context.Context, loader any) context.Context {
	return context.WithValue(ctx, dataLoaderKey, loader)
}

func GetDataLoader(ctx context.Context) (any, bool) {
	value := ctx.Value(dataLoaderKey)
	return value, value != nil
}

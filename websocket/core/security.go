package core

import (
	"context"
	"errors"
	"time"
)

var (
	ErrUnauthorized    = errors.New("websocket operation unauthorized")
	ErrPayloadTooLarge = errors.New("websocket event payload too large")
	ErrInvalidPayload  = errors.New("websocket event payload invalid")
	ErrRoomLimit       = errors.New("websocket room limit reached")
	// ErrIdentityExpired means the identity's ExpiresAt has already passed.
	ErrIdentityExpired = errors.New("websocket identity expired")
)

// Identity is the authenticated principal attached to a physical connection.
type Identity struct {
	UserID    string
	TenantID  string
	AccountID string
	SessionID string
	ExpiresAt time.Time
}

// Authorizer controls client event publication and room membership.
type Authorizer interface {
	CanSubscribe(context.Context, Identity, string) error
	CanPublish(context.Context, Identity, string) error
}

type allowAuthorizer struct{}

func (allowAuthorizer) CanSubscribe(context.Context, Identity, string) error { return nil }
func (allowAuthorizer) CanPublish(context.Context, Identity, string) error   { return nil }

// DispatchPolicy selects local callbacks or a durable business command publisher.
type DispatchPolicy int

const (
	Local DispatchPolicy = iota
	Business
)

// HandlerConfig defines validation and execution policy for an event type.
type HandlerConfig struct {
	Dispatch        DispatchPolicy
	MaxPayloadBytes int
	Timeout         time.Duration
	Validate        func([]byte) error
	Handler         ContextJobHandler
}

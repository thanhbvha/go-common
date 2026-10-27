package auth

import (
	"context"
	"errors"
	"fmt"

	"github.com/goccy/go-json"
)

var ErrAADRequired = errors.New("auth: additional authenticated data is required")

// AADPolicy validates Additional Authenticated Data before encryption and
// decryption. Implementations must be safe for concurrent use.
type AADPolicy interface {
	ValidateAAD(ctx context.Context, aad []byte) error
}

// AADPolicyFunc adapts a function into an AADPolicy.
type AADPolicyFunc func(context.Context, []byte) error

func (f AADPolicyFunc) ValidateAAD(ctx context.Context, aad []byte) error {
	return f(ctx, aad)
}

// RequireAAD rejects empty token-binding data.
var RequireAAD AADPolicy = AADPolicyFunc(func(_ context.Context, aad []byte) error {
	if len(aad) == 0 {
		return ErrAADRequired
	}
	return nil
})

// AADContext is a canonical application binding for encrypted tokens.
type AADContext struct {
	Issuer    string `json:"issuer,omitempty"`
	Audience  string `json:"audience,omitempty"`
	SessionID string `json:"session_id"`
	DeviceID  string `json:"device_id,omitempty"`
}

// Bytes returns deterministic JSON and avoids ambiguous string concatenation.
func (a AADContext) Bytes() ([]byte, error) {
	if a.SessionID == "" {
		return nil, fmt.Errorf("%w: session ID is required", ErrAADRequired)
	}
	return json.Marshal(a)
}

func validateAAD(ctx context.Context, policy AADPolicy, aad []byte) error {
	if policy == nil {
		return nil
	}
	if err := policy.ValidateAAD(ctx, aad); err != nil {
		return fmt.Errorf("auth: AAD policy rejected value: %w", err)
	}
	return nil
}

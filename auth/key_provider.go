package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrKeyNotFound  = errors.New("auth: key not found")
	ErrInvalidKeyID = errors.New("auth: key ID must not be empty")
)

// KeyMaterial is a versioned cryptographic key. IDs are written to token
// headers/claims and must remain bound to the same bytes for their lifetime.
type KeyMaterial struct {
	ID    string
	Value []byte
}

// KeyProvider supplies the active key for issuance and resolves historical
// keys during validation. Implementations must be safe for concurrent use.
type KeyProvider interface {
	CurrentKey(ctx context.Context) (KeyMaterial, error)
	Key(ctx context.Context, keyID string) ([]byte, error)
}

// StaticKeyProvider is an in-memory, immutable keyring.
type StaticKeyProvider struct {
	activeID string
	keys     map[string][]byte
}

// NewStaticKeyProvider creates an immutable keyring. Tokens must carry kid.
func NewStaticKeyProvider(activeID string, keys map[string][]byte) (*StaticKeyProvider, error) {
	if strings.TrimSpace(activeID) == "" {
		return nil, ErrInvalidKeyID
	}
	if len(keys) == 0 {
		return nil, ErrInvalidKey
	}
	copied := make(map[string][]byte, len(keys))
	for id, key := range keys {
		if strings.TrimSpace(id) == "" || len(key) == 0 {
			return nil, ErrInvalidKey
		}
		copied[id] = append([]byte(nil), key...)
	}
	if _, ok := copied[activeID]; !ok {
		return nil, fmt.Errorf("%w: active key %q", ErrKeyNotFound, activeID)
	}
	return &StaticKeyProvider{activeID: activeID, keys: copied}, nil
}

func (p *StaticKeyProvider) CurrentKey(context.Context) (KeyMaterial, error) {
	key, ok := p.keys[p.activeID]
	if !ok {
		return KeyMaterial{}, ErrKeyNotFound
	}
	return KeyMaterial{ID: p.activeID, Value: append([]byte(nil), key...)}, nil
}

func (p *StaticKeyProvider) Key(_ context.Context, keyID string) ([]byte, error) {
	if strings.TrimSpace(keyID) == "" {
		return nil, ErrInvalidKeyID
	}
	key, ok := p.keys[keyID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrKeyNotFound, keyID)
	}
	return append([]byte(nil), key...), nil
}

func singleKeyProvider(value string) (KeyProvider, error) {
	return NewStaticKeyProvider("default", map[string][]byte{"default": []byte(value)})
}

func currentKey(ctx context.Context, provider KeyProvider, exactSize bool) (KeyMaterial, error) {
	if isNilValue(provider) {
		return KeyMaterial{}, ErrInvalidKey
	}
	key, err := provider.CurrentKey(ctx)
	if err != nil {
		return KeyMaterial{}, fmt.Errorf("auth: load current key: %w", err)
	}
	if strings.TrimSpace(key.ID) == "" || !validKeyLength(key.Value, exactSize) {
		return KeyMaterial{}, ErrInvalidKey
	}
	return key, nil
}

func resolveKey(ctx context.Context, provider KeyProvider, keyID string, exactSize bool) ([]byte, error) {
	if strings.TrimSpace(keyID) == "" {
		return nil, ErrInvalidKeyID
	}
	if isNilValue(provider) {
		return nil, ErrInvalidKey
	}
	key, err := provider.Key(ctx, keyID)
	if err != nil {
		return nil, err
	}
	if !validKeyLength(key, exactSize) {
		return nil, ErrInvalidKey
	}
	return key, nil
}

func validKeyLength(key []byte, exactSize bool) bool {
	if exactSize {
		return len(key) == 32
	}
	return len(key) >= 32
}

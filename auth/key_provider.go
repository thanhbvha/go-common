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
	legacyID string
	keys     map[string][]byte
}

// NewStaticKeyProvider creates an immutable keyring. When there is exactly one
// key, validation of legacy tokens without kid falls back to that key.
func NewStaticKeyProvider(activeID string, keys map[string][]byte) (*StaticKeyProvider, error) {
	legacyID := ""
	if len(keys) == 1 {
		legacyID = activeID
	}
	return NewStaticKeyProviderWithLegacy(activeID, legacyID, keys)
}

// NewStaticKeyProviderWithLegacy configures the key used for tokens created
// before kid was introduced. Pass an empty legacyID to reject such tokens.
func NewStaticKeyProviderWithLegacy(activeID, legacyID string, keys map[string][]byte) (*StaticKeyProvider, error) {
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
	if legacyID != "" {
		if _, ok := copied[legacyID]; !ok {
			return nil, fmt.Errorf("%w: legacy key %q", ErrKeyNotFound, legacyID)
		}
	}
	return &StaticKeyProvider{activeID: activeID, legacyID: legacyID, keys: copied}, nil
}

func (p *StaticKeyProvider) CurrentKey(context.Context) (KeyMaterial, error) {
	key, ok := p.keys[p.activeID]
	if !ok {
		return KeyMaterial{}, ErrKeyNotFound
	}
	return KeyMaterial{ID: p.activeID, Value: append([]byte(nil), key...)}, nil
}

func (p *StaticKeyProvider) Key(_ context.Context, keyID string) ([]byte, error) {
	if keyID == "" {
		keyID = p.legacyID
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
	if provider == nil {
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
	if provider == nil {
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

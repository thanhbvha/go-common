package pubsub

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	// WireVersion is the only CrossNodeMessage version this build accepts.
	WireVersion = 1
	// DefaultMaxMessageBytes bounds a broker payload before any decoding.
	DefaultMaxMessageBytes = 256 << 10
	maxIdentifierBytes     = 256
)

var (
	ErrMessageTooLarge  = errors.New("websocket pubsub: message exceeds size limit")
	ErrInvalidMessage   = errors.New("websocket pubsub: invalid message")
	ErrUnauthenticated  = errors.New("websocket pubsub: message authentication failed")
	ErrInvalidAuthority = errors.New("websocket pubsub: invalid authenticator")
)

var knownTypes = map[string]struct{}{
	MessageTypeChat: {}, MessageTypeChatRoom: {}, MessageTypeNotification: {}, MessageTypeBroadcast: {},
	MessageTypeUserJoin: {}, MessageTypeUserLeave: {}, MessageTypeShardCreate: {}, MessageTypeShardDestroy: {},
	MessageTypeNodeStatus: {},
}

// DecodeCrossNodeMessage treats payload as untrusted. The size limit is applied
// before decoding, decoding happens exactly once, and structurally invalid
// messages are rejected. maxBytes <= 0 selects DefaultMaxMessageBytes.
func DecodeCrossNodeMessage(payload []byte, maxBytes int) (*CrossNodeMessage, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxMessageBytes
	}
	if len(payload) > maxBytes {
		return nil, ErrMessageTooLarge
	}
	var message CrossNodeMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	if err := message.validate(); err != nil {
		return nil, err
	}
	return &message, nil
}

func (m *CrossNodeMessage) validate() error {
	if m.Version != WireVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrInvalidMessage, m.Version)
	}
	if _, ok := knownTypes[m.Type]; !ok {
		return fmt.Errorf("%w: unknown type", ErrInvalidMessage)
	}
	for _, id := range [...]string{m.ID, m.NodeID, m.ShardID, m.UserID, m.RoomID} {
		if len(id) > maxIdentifierBytes {
			return fmt.Errorf("%w: identifier too long", ErrInvalidMessage)
		}
	}
	if m.NodeID == "" {
		return fmt.Errorf("%w: missing node id", ErrInvalidMessage)
	}
	switch m.Type {
	case MessageTypeNotification, MessageTypeChat:
		if m.UserID == "" || m.ShardID == "" {
			return fmt.Errorf("%w: %s requires user and shard", ErrInvalidMessage, m.Type)
		}
	case MessageTypeChatRoom:
		if m.RoomID == "" || m.ShardID == "" {
			return fmt.Errorf("%w: chat_room requires room and shard", ErrInvalidMessage)
		}
	case MessageTypeBroadcast:
		if m.ShardID == "" {
			return fmt.Errorf("%w: broadcast requires shard", ErrInvalidMessage)
		}
	}
	return nil
}

// Payload extracts the deliverable client payload, reporting false when the
// message carries none so callers never emit an empty frame.
func (m *CrossNodeMessage) Payload() ([]byte, bool) {
	value, ok := m.Data["message"]
	if !ok || value == nil {
		return nil, false
	}
	if text, ok := value.(string); ok {
		if text == "" {
			return nil, false
		}
		return []byte(text), true
	}
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) == 0 {
		return nil, false
	}
	return encoded, true
}

// AuthMode selects how messages are signed and how strictly inbound ones are
// verified. A message carrying a signature must always verify, in every mode.
//
// Rolling upgrade of a live cluster takes three steps, because a node that
// cannot verify signatures cannot read signed replies:
//  1. AuthPassive on every node: accept signed and unsigned, send unsigned.
//  2. AuthOptional on every node: accept both, send signed.
//  3. AuthRequired on every node: reject unsigned.
type AuthMode int

const (
	// AuthRequired signs outbound and rejects unsigned or wrongly signed inbound.
	AuthRequired AuthMode = iota
	// AuthOptional signs outbound and accepts unsigned inbound.
	AuthOptional
	// AuthPassive sends unsigned and accepts unsigned or validly signed inbound.
	AuthPassive
)

type signedEnvelope struct {
	KeyID   string          `json:"kid"`
	Sig     string          `json:"sig"`
	Payload json.RawMessage `json:"payload"`
}

// Authenticator signs outbound broker payloads and verifies inbound ones with
// HMAC-SHA256 over the exact transmitted bytes. Keys are rotated by adding a new
// key id and switching the signing key once every node accepts both.
type Authenticator struct {
	signingID string
	keys      map[string][]byte
	mode      AuthMode
}

// NewAuthenticator signs with keys[signingID] and verifies with any key in keys.
// Keys must be at least 32 bytes.
func NewAuthenticator(signingID string, keys map[string][]byte, mode AuthMode) (*Authenticator, error) {
	if signingID == "" || len(keys[signingID]) == 0 {
		return nil, ErrInvalidAuthority
	}
	copied := make(map[string][]byte, len(keys))
	for id, key := range keys {
		if id == "" || len(key) < 32 {
			return nil, fmt.Errorf("%w: key %q must be at least 32 bytes", ErrInvalidAuthority, id)
		}
		copied[id] = append([]byte(nil), key...)
	}
	return &Authenticator{signingID: signingID, keys: copied, mode: mode}, nil
}

// Strict reports whether this node signs its output and rejects unsigned input
// (AuthRequired). Production validation uses it to refuse a cluster that would
// accept forged broker messages.
func (a *Authenticator) Strict() bool { return a != nil && a.mode == AuthRequired }

func (a *Authenticator) mac(key, payload []byte) string {
	h := hmac.New(sha256.New, key)
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// Seal wraps payload in a signed envelope. A nil Authenticator returns payload
// unchanged, so callers need no branching.
func (a *Authenticator) Seal(payload []byte) ([]byte, error) {
	if a == nil || a.mode == AuthPassive {
		return payload, nil
	}
	return json.Marshal(signedEnvelope{KeyID: a.signingID, Sig: a.mac(a.keys[a.signingID], payload), Payload: payload})
}

// Open verifies wire and returns the inner payload. maxBytes bounds the
// envelope before parsing. A nil Authenticator returns wire unchanged.
func (a *Authenticator) Open(wire []byte, maxBytes int) ([]byte, error) {
	if a == nil {
		return wire, nil
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxMessageBytes
	}
	if len(wire) > maxBytes+1024 {
		return nil, ErrMessageTooLarge
	}
	var envelope signedEnvelope
	// Unsigned legacy messages have no "sig" field. Anything carrying one must
	// verify, in every mode, so an attacker cannot downgrade by corrupting it.
	if err := json.Unmarshal(wire, &envelope); err != nil || envelope.Sig == "" {
		if a.mode == AuthOptional || a.mode == AuthPassive {
			return wire, nil
		}
		return nil, ErrUnauthenticated
	}
	if len(envelope.Payload) == 0 {
		return nil, ErrUnauthenticated
	}
	key, ok := a.keys[envelope.KeyID]
	if !ok {
		return nil, ErrUnauthenticated
	}
	expected, err := hex.DecodeString(a.mac(key, envelope.Payload))
	provided, decodeErr := hex.DecodeString(envelope.Sig)
	if err != nil || decodeErr != nil || !hmac.Equal(expected, provided) {
		return nil, ErrUnauthenticated
	}
	return envelope.Payload, nil
}

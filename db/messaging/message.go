// Package messaging defines shared messages, identities and observations for
// transactional outbox/inbox. Core contracts live in child packages; database
// and broker dependencies belong only in adapters.
package messaging

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const MaxPayloadBytes = 1 << 20

type Kind string

const (
	Task  Kind = "task"
	Event Kind = "event"
)

// Message is immutable after Append. AvailableAt gates DB dispatch, not a broker
// scheduler. IDs must be generated before a retryable application transaction.
type Message struct {
	ID             string
	Namespace      string
	TenantID       string
	Kind           Kind
	Type           string
	SchemaVersion  int
	Payload        json.RawMessage
	Headers        map[string]string
	OrderingKey    string
	ConcurrencyKey string
	AvailableAt    time.Time
}

func ValidIdentity(s string, empty bool) bool {
	if s == "" {
		return empty
	}
	if len(s) > 256 || !utf8.ValidString(s) || s != strings.TrimSpace(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func (m Message) Validate() error {
	if !ValidIdentity(m.ID, false) || !ValidIdentity(m.Namespace, false) || !ValidIdentity(m.TenantID, true) ||
		!ValidIdentity(m.Type, false) || (m.Kind != Task && m.Kind != Event) || m.SchemaVersion < 1 ||
		!ValidIdentity(m.OrderingKey, true) || !ValidIdentity(m.ConcurrencyKey, true) ||
		len(m.Payload) == 0 || len(m.Payload) > MaxPayloadBytes || !utf8.Valid(m.Payload) || !json.Valid(m.Payload) {
		return ErrInvalidMessage
	}
	if len(m.Headers) > 128 {
		return ErrInvalidMessage
	}
	bytes := 0
	for k, v := range m.Headers {
		bytes += len(k) + len(v)
		if !ValidIdentity(k, false) || !utf8.ValidString(v) || strings.ContainsRune(v, '\x00') {
			return ErrInvalidMessage
		}
	}
	if bytes > 32<<10 {
		return ErrInvalidMessage
	}
	if !m.AvailableAt.IsZero() && (m.AvailableAt.Year() < 1 || m.AvailableAt.Year() > 9999) {
		return ErrInvalidMessage
	}
	return nil
}

// Fingerprint ignores delivery metadata. Object keys are sorted, whitespace is
// normalized, and json.Number preserves integers above 2^53. Numeric spellings
// such as 1 and 1.0 intentionally remain distinct business representations.
func (m Message) Fingerprint() (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	d := json.NewDecoder(bytes.NewReader(m.Payload))
	d.UseNumber()
	var value any
	if err := d.Decode(&value); err != nil {
		return "", ErrInvalidMessage
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return "", ErrInvalidMessage
	}
	b, err := json.Marshal([]any{m.Kind, m.Type, m.SchemaVersion, value})
	if err != nil {
		return "", ErrInvalidMessage
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

type Identity struct{ Namespace, TenantID, ID string }

func (m Message) Identity() Identity { return Identity{m.Namespace, m.TenantID, m.ID} }

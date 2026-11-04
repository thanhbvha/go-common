package messaging

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func testMessage() Message {
	return Message{ID: "stable", Namespace: "test", Kind: Event, Type: "created", SchemaVersion: 1, Payload: []byte(`{"id":9223372036854775807,"amount":1}`)}
}
func TestFingerprintPrecisionAndMetadata(t *testing.T) {
	m := testMessage()
	first, err := m.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	m.Payload = []byte(" { \"amount\": 1, \"id\": 9223372036854775807 } ")
	m.Headers = map[string]string{"traceparent": "different"}
	m.AvailableAt = time.Now()
	second, err := m.Fingerprint()
	if err != nil || first != second {
		t.Fatal("JSON or delivery metadata changed fingerprint", err)
	}
	m.Payload = []byte(`{"id":9223372036854775806,"amount":1}`)
	third, _ := m.Fingerprint()
	if first == third {
		t.Fatal("integer precision lost")
	}
	m.SchemaVersion++
	fourth, _ := m.Fingerprint()
	if fourth == third {
		t.Fatal("version excluded")
	}
}
func TestMessageBounds(t *testing.T) {
	for _, mutate := range []func(*Message){func(m *Message) { m.ID = "" }, func(m *Message) { m.Kind = "other" }, func(m *Message) { m.Payload = []byte(`{} {}`) }, func(m *Message) { m.Payload = []byte(strings.Repeat(" ", MaxPayloadBytes+1)) }, func(m *Message) { m.Namespace = " bad" }, func(m *Message) { m.SchemaVersion = 0 }, func(m *Message) { m.Headers = map[string]string{"a": "\x00"} }} {
		m := testMessage()
		mutate(&m)
		if !errors.Is(m.Validate(), ErrInvalidMessage) {
			t.Fatal("invalid message accepted")
		}
	}
}

type panicObserver struct{}

func (panicObserver) Observe(context.Context, Observation) { panic("secret") }
func TestObserverIsolation(t *testing.T)                   { Observe(t.Context(), panicObserver{}, Observation{}) }

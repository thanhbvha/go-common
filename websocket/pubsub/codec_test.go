package pubsub

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func validMessage(mutate func(*CrossNodeMessage)) []byte {
	m := CrossNodeMessage{ID: "1", Version: WireVersion, Type: MessageTypeBroadcast, NodeID: "node-a", ShardID: "s", Data: map[string]interface{}{"message": "hi"}}
	if mutate != nil {
		mutate(&m)
	}
	b, _ := json.Marshal(m)
	return b
}

func TestDecodeRejectsOversizedBeforeParsing(t *testing.T) {
	huge := []byte(`{"id":"` + strings.Repeat("a", 2048) + `"}`)
	if _, err := DecodeCrossNodeMessage(huge, 1024); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("err = %v", err)
	}
}

func TestDecodeRejectsStructurallyInvalidMessages(t *testing.T) {
	cases := map[string][]byte{
		"not json":        []byte(`{`),
		"bad version":     validMessage(func(m *CrossNodeMessage) { m.Version = 9 }),
		"unknown type":    validMessage(func(m *CrossNodeMessage) { m.Type = "evil" }),
		"no node":         validMessage(func(m *CrossNodeMessage) { m.NodeID = "" }),
		"long identifier": validMessage(func(m *CrossNodeMessage) { m.RoomID = strings.Repeat("r", 300) }),
		"notification without user": validMessage(func(m *CrossNodeMessage) {
			m.Type = MessageTypeNotification
		}),
		"room without room": validMessage(func(m *CrossNodeMessage) { m.Type = MessageTypeChatRoom }),
		"broadcast without shard": validMessage(func(m *CrossNodeMessage) {
			m.ShardID = ""
		}),
	}
	for name, payload := range cases {
		if _, err := DecodeCrossNodeMessage(payload, 0); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := DecodeCrossNodeMessage(validMessage(nil), 0); err != nil {
		t.Fatalf("valid message rejected: %v", err)
	}
}

func TestPayloadNeverYieldsEmptyFrame(t *testing.T) {
	for name, data := range map[string]map[string]interface{}{
		"missing": nil, "nil value": {"message": nil}, "empty string": {"message": ""},
	} {
		if p, ok := (&CrossNodeMessage{Data: data}).Payload(); ok || p != nil {
			t.Errorf("%s produced payload %q", name, p)
		}
	}
	if p, ok := (&CrossNodeMessage{Data: map[string]interface{}{"message": map[string]any{"a": 1}}}).Payload(); !ok || string(p) != `{"a":1}` {
		t.Fatalf("structured payload = %q %v", p, ok)
	}
}

func testKeys() map[string][]byte {
	return map[string][]byte{"k1": bytes.Repeat([]byte{1}, 32), "k2": bytes.Repeat([]byte{2}, 32)}
}

func TestAuthenticatorRoundTripAndTamper(t *testing.T) {
	auth, err := NewAuthenticator("k1", testKeys(), AuthRequired)
	if err != nil {
		t.Fatal(err)
	}
	payload := validMessage(nil)
	sealed, err := auth.Seal(payload)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := auth.Open(sealed, 0)
	if err != nil || !bytes.Equal(opened, payload) {
		t.Fatalf("open = %q, %v", opened, err)
	}
	tampered := bytes.Replace(sealed, []byte("node-a"), []byte("node-b"), 1)
	if _, err := auth.Open(tampered, 0); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("tampered payload accepted: %v", err)
	}
}

func TestAuthenticatorRequiredRejectsUnsignedAndUnknownKey(t *testing.T) {
	auth, _ := NewAuthenticator("k1", testKeys(), AuthRequired)
	if _, err := auth.Open(validMessage(nil), 0); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("unsigned accepted: %v", err)
	}
	other, _ := NewAuthenticator("x", map[string][]byte{"x": bytes.Repeat([]byte{9}, 32)}, AuthRequired)
	sealed, _ := other.Seal(validMessage(nil))
	if _, err := auth.Open(sealed, 0); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("unknown key accepted: %v", err)
	}
}

func TestAuthenticatorOptionalAcceptsUnsignedButNotBadSignature(t *testing.T) {
	auth, _ := NewAuthenticator("k1", testKeys(), AuthOptional)
	unsigned := validMessage(nil)
	if got, err := auth.Open(unsigned, 0); err != nil || !bytes.Equal(got, unsigned) {
		t.Fatalf("unsigned rejected in optional mode: %v", err)
	}
	sealed, _ := auth.Seal(unsigned)
	bad := bytes.Replace(sealed, []byte(`"sig":"`), []byte(`"sig":"00`), 1)
	if _, err := auth.Open(bad, 0); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("bad signature downgraded to unsigned: %v", err)
	}
}

func TestAuthenticatorPassiveSendsUnsignedButVerifiesSigned(t *testing.T) {
	passive, _ := NewAuthenticator("k1", testKeys(), AuthPassive)
	payload := validMessage(nil)
	if out, err := passive.Seal(payload); err != nil || !bytes.Equal(out, payload) {
		t.Fatalf("passive node signed its output: %q %v", out, err)
	}
	signer, _ := NewAuthenticator("k1", testKeys(), AuthRequired)
	sealed, _ := signer.Seal(payload)
	if out, err := passive.Open(sealed, 0); err != nil || !bytes.Equal(out, payload) {
		t.Fatalf("passive node rejected a valid signed message: %v", err)
	}
	tampered := bytes.Replace(sealed, []byte("node-a"), []byte("node-b"), 1)
	if _, err := passive.Open(tampered, 0); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("passive node accepted a forged signed message: %v", err)
	}
}

func TestAuthenticatorKeyRotation(t *testing.T) {
	oldSigner, _ := NewAuthenticator("k1", testKeys(), AuthRequired)
	newSigner, _ := NewAuthenticator("k2", testKeys(), AuthRequired)
	sealed, _ := oldSigner.Seal(validMessage(nil))
	if _, err := newSigner.Open(sealed, 0); err != nil {
		t.Fatalf("message from previous key rejected during rotation: %v", err)
	}
}

func TestAuthenticatorRejectsWeakKeys(t *testing.T) {
	if _, err := NewAuthenticator("k", map[string][]byte{"k": []byte("short")}, AuthRequired); err == nil {
		t.Fatal("weak key accepted")
	}
	if _, err := NewAuthenticator("missing", testKeys(), AuthRequired); err == nil {
		t.Fatal("missing signing key accepted")
	}
}

func TestNilAuthenticatorIsPassthrough(t *testing.T) {
	var auth *Authenticator
	in := []byte("x")
	if out, err := auth.Seal(in); err != nil || !bytes.Equal(out, in) {
		t.Fatal("seal not passthrough")
	}
	if out, err := auth.Open(in, 0); err != nil || !bytes.Equal(out, in) {
		t.Fatal("open not passthrough")
	}
}

func FuzzDecodeCrossNodeMessage(f *testing.F) {
	f.Add(validMessage(nil))
	f.Add([]byte(`{"version":1,"type":"broadcast"}`))
	f.Fuzz(func(t *testing.T, payload []byte) {
		message, err := DecodeCrossNodeMessage(payload, 4096)
		if err == nil && (message == nil || message.Version != WireVersion) {
			t.Fatalf("accepted invalid message %+v", message)
		}
	})
}

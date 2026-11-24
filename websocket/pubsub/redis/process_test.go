package redispubsub

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
)

func inbound(mutate func(*pubsub.CrossNodeMessage)) string {
	m := pubsub.CrossNodeMessage{ID: "1", Version: pubsub.WireVersion, Type: pubsub.MessageTypeBroadcast, NodeID: "peer", ShardID: "s", Data: map[string]interface{}{"message": "x"}}
	if mutate != nil {
		mutate(&m)
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func newRecordingManager(t *testing.T, options ...Option) (*Manager, *atomic.Int32) {
	t.Helper()
	manager := NewManagerWithClientAndPolicy(nil, pubsub.AllowStandalone, options...)
	t.Cleanup(func() { _ = manager.Shutdown() })
	var calls atomic.Int32
	manager.RegisterHandler(pubsub.MessageTypeBroadcast, func(*pubsub.CrossNodeMessage) { calls.Add(1) })
	return manager, &calls
}

func waitCalls(calls *atomic.Int32, want int32) bool {
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if calls.Load() == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return calls.Load() == want
}

func TestProcessMessageDispatchesValidAndRejectsInvalid(t *testing.T) {
	manager, calls := newRecordingManager(t)
	manager.processMessage("shard:s", inbound(nil))
	if !waitCalls(calls, 1) {
		t.Fatal("valid message not dispatched")
	}
	for name, payload := range map[string]string{
		"garbage":      "{",
		"bad version":  inbound(func(m *pubsub.CrossNodeMessage) { m.Version = 7 }),
		"unknown type": inbound(func(m *pubsub.CrossNodeMessage) { m.Type = "evil" }),
	} {
		manager.processMessage("shard:s", payload)
		if name == "" {
			t.Fatal(name)
		}
	}
	if got := manager.rejected.Load(); got != 3 {
		t.Fatalf("rejected = %d, want 3", got)
	}
	if !waitCalls(calls, 1) {
		t.Fatalf("invalid messages reached a handler: %d calls", calls.Load())
	}
}

func TestProcessMessageEnforcesSizeLimitBeforeDecoding(t *testing.T) {
	manager, calls := newRecordingManager(t, WithMaxMessageBytes(256))
	manager.processMessage("shard:s", inbound(func(m *pubsub.CrossNodeMessage) {
		m.Data = map[string]interface{}{"message": strings.Repeat("a", 1024)}
	}))
	if manager.rejected.Load() != 1 || calls.Load() != 0 {
		t.Fatalf("oversized message: rejected=%d calls=%d", manager.rejected.Load(), calls.Load())
	}
}

func TestProcessMessageIgnoresOwnNode(t *testing.T) {
	manager, calls := newRecordingManager(t)
	manager.processMessage("shard:s", inbound(func(m *pubsub.CrossNodeMessage) { m.NodeID = manager.nodeID }))
	if !waitCalls(calls, 0) || manager.rejected.Load() != 0 {
		t.Fatal("own message was dispatched or counted as rejected")
	}
}

func TestProcessMessageRequiresValidSignature(t *testing.T) {
	keys := map[string][]byte{"k": bytes.Repeat([]byte{7}, 32)}
	auth, err := pubsub.NewAuthenticator("k", keys, pubsub.AuthRequired)
	if err != nil {
		t.Fatal(err)
	}
	manager, calls := newRecordingManager(t, WithAuthenticator(auth))

	manager.processMessage("shard:s", inbound(nil))
	if manager.rejected.Load() != 1 || calls.Load() != 0 {
		t.Fatalf("unsigned message accepted: rejected=%d calls=%d", manager.rejected.Load(), calls.Load())
	}
	sealed, err := auth.Seal([]byte(inbound(nil)))
	if err != nil {
		t.Fatal(err)
	}
	manager.processMessage("shard:s", string(sealed))
	if !waitCalls(calls, 1) {
		t.Fatal("signed message rejected")
	}
	forged := strings.Replace(string(sealed), `"peer"`, `"mallory"`, 1)
	manager.processMessage("shard:s", forged)
	if manager.rejected.Load() != 2 {
		t.Fatalf("forged message accepted, rejected=%d", manager.rejected.Load())
	}
}

package redispubsub

import (
	"testing"

	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
)

// FuzzProcessMessage sends arbitrary broker payloads through the full inbound
// path. Hostile input must be rejected or dispatched, never panic.
func FuzzProcessMessage(f *testing.F) {
	f.Add(inbound(nil))
	f.Add("{")
	f.Add(`{"version":1,"type":"broadcast","data":{"message":1}}`)
	f.Add("\xff\xfe")
	manager := NewManagerWithClientAndPolicy(nil, pubsub.AllowStandalone)
	f.Cleanup(func() { _ = manager.Shutdown() })
	manager.RegisterHandler(pubsub.MessageTypeBroadcast, func(*pubsub.CrossNodeMessage) {})
	f.Fuzz(func(t *testing.T, payload string) { manager.processMessage("shard:s", payload) })
}

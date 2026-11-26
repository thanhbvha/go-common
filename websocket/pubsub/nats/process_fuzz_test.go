package natspubsub

import (
	"testing"

	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
)

// FuzzProcessMessage sends arbitrary broker payloads through the full inbound
// path. Hostile input must be rejected or dispatched, never panic.
func FuzzProcessMessage(f *testing.F) {
	f.Add([]byte(`{"id":"1","version":1,"type":"broadcast","nodeId":"peer","shardId":"s","data":{"message":"x"}}`))
	f.Add([]byte("{"))
	f.Add([]byte{0xff, 0xfe})
	manager := NewManagerWithClientAndPolicy(nil, pubsub.AllowStandalone)
	f.Cleanup(func() { _ = manager.Shutdown() })
	manager.RegisterHandler(pubsub.MessageTypeBroadcast, func(*pubsub.CrossNodeMessage) {})
	f.Fuzz(func(t *testing.T, payload []byte) { manager.processMessage("shard:s", payload) })
}

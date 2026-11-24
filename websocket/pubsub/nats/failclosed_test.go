package natspubsub

import (
	"testing"

	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
)

func TestNilClientIsUnreadyByDefaultNotSilentlyStandalone(t *testing.T) {
	manager := NewManagerWithClient(nil)
	t.Cleanup(func() { _ = manager.Shutdown() })

	if manager.Ready() {
		t.Fatal("a manager without a broker reported ready")
	}
	if err := manager.Publish("shard:s", &pubsub.CrossNodeMessage{Type: pubsub.MessageTypeBroadcast, ShardID: "s"}); err == nil {
		t.Fatal("publish succeeded without a broker; messages would be silently dropped or looped back")
	}
	if err := manager.Subscribe("shard:s"); err == nil {
		t.Fatal("subscribe succeeded without a broker")
	}
}

func TestStandaloneIsAnExplicitOptIn(t *testing.T) {
	manager := NewManagerWithClientAndPolicy(nil, pubsub.AllowStandalone)
	t.Cleanup(func() { _ = manager.Shutdown() })
	if !manager.Ready() {
		t.Fatal("explicit standalone manager should be ready")
	}
}

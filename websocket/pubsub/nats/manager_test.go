package natspubsub

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/websocket/pubsub"
)

func TestLoopbackHandlerContinuesAfterPanic(t *testing.T) {
	manager := NewManagerWithClient(nil)
	t.Cleanup(func() { _ = manager.Shutdown() })

	var calls atomic.Int32
	done := make(chan struct{})
	manager.RegisterHandler("panic-test", func(*pubsub.CrossNodeMessage) {
		if calls.Add(1) == 1 {
			panic("synthetic nats loopback panic")
		}
		close(done)
	})

	message := func() *pubsub.CrossNodeMessage {
		return &pubsub.CrossNodeMessage{Type: "panic-test", ShardID: "shard-1"}
	}
	if err := manager.Publish("shard:shard-1", message()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := manager.Publish("shard:shard-1", message()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("nats loopback handler did not continue after panic")
	}
}

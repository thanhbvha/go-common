package redispubsub

import (
	"testing"

	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
)

func TestWithExecutorSizesDispatch(t *testing.T) {
	m := NewManagerWithClientAndPolicy(nil, pubsub.AllowStandalone, WithExecutor(2, 8))
	t.Cleanup(func() { _ = m.Shutdown() })
	if s := m.executor.Stats(); s.Partitions != 2 || s.QueueSize != 2*8 {
		t.Fatalf("executor = %d x %d, want 2 x 8 (QueueSize is the total)", s.Partitions, s.QueueSize)
	}
	d := NewManagerWithClientAndPolicy(nil, pubsub.AllowStandalone)
	t.Cleanup(func() { _ = d.Shutdown() })
	if s := d.executor.Stats(); s.Partitions != 4 || s.QueueSize != 4*256 {
		t.Fatalf("default executor = %d x %d, want 4 x 256", s.Partitions, s.QueueSize)
	}
}

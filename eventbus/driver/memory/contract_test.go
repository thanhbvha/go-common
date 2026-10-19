package memory

import (
	"context"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/thanhbvha/go-common/eventbus/transport/contracttest"
)

func TestMemoryTransportContract(t *testing.T) {
	contracttest.Run(t, contracttest.Harness{
		New: func(t *testing.T) transport.Transport {
			backend := New()
			t.Cleanup(func() { _ = backend.Close(context.Background()) })
			return backend
		},
		Topology: func(name string, kind transport.TopologyKind) transport.Topology {
			return transport.Topology{Namespace: "memory-contract", JobType: name, Kind: kind}
		},
		DispatchDue: func(ctx context.Context, tr transport.Transport, _ transport.Topology, now time.Time) (int64, error) {
			return tr.(*memoryTransport).dispatchDue(ctx, now)
		},
		ExpireDedupe:  func(_ *testing.T, duration time.Duration) { time.Sleep(duration) },
		ReceiveWithin: time.Second,
	})
}

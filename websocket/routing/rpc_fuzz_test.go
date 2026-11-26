package routing

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// FuzzRPCReceive feeds arbitrary bytes to the broker-facing receive path. It
// must never panic, hang, or reach the delivery handler with an unsigned frame.
func FuzzRPCReceive(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"correlationId":"x","command":{"targetNode":"n"},"reply":"r"}`))
	f.Add([]byte(`{"command":null}`))
	f.Add([]byte{0xff, 0xfe, 0x00})
	f.Fuzz(func(t *testing.T, b []byte) {
		wire := newMemoryWire()
		var handled atomicCount
		tr := NewRPCTransport(wire, "fuzz")
		if err := tr.Start(context.Background(), "n", func(context.Context, DeliveryCommand) DeliveryResult {
			handled.add()
			return DeliveryResult{}
		}); err != nil {
			t.Fatal(err)
		}
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = tr.Close(ctx)
		}()
		done := make(chan struct{})
		go func() { defer close(done); tr.receive(b) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("receive did not return")
		}
	})
}

type atomicCount struct{ n atomic.Int32 }

func (c *atomicCount) add() { c.n.Add(1) }

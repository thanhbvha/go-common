package routing

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Measures gateway coordination and immutable fan-out with nonblocking no-op
// enqueues. It excludes brokers, socket I/O and client receipt.
func BenchmarkGatewayDelivery(b *testing.B) {
	for _, recipients := range []int{1, 1000} {
		for _, parallel := range []bool{false, true} {
			b.Run(fmt.Sprintf("recipients=%d/parallel=%t", recipients, parallel), func(b *testing.B) {
				g, _ := NewGateway(GatewayConfig{NodeID: "node", Registry: &renewalRegistry{}, Transport: readyTransport{}})
				defer g.cancel()
				g.leaseUntil.Store(time.Now().Add(time.Hour).UnixNano())
				var locations []Location
				for i := range recipients {
					l := Location{TenantID: "t", UserID: fmt.Sprint(i), SessionID: "s", NodeID: "node", Generation: "g"}
					g.installLocal(&localSession{location: l, enqueue: func([]byte) error { return nil }, rooms: map[string]struct{}{"r": {}}})
					locations = append(locations, l)
				}
				run := func() {
					c := DeliveryCommand{Version: 1, MessageID: NewID(), TenantID: "t", TargetNode: "node", Payload: []byte("hello"), ExpiresAt: time.Now().Add(time.Hour)}
					if recipients == 1 {
						c.Sessions = locations
					} else {
						c.RoomID = "r"
					}
					if res := g.Handle(context.Background(), c); res.Count(Enqueued) != recipients {
						b.Fatalf("recipient count: %+v", res)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				if parallel {
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							run()
						}
					})
				} else {
					for i := 0; i < b.N; i++ {
						run()
					}
				}
			})
		}
	}
}

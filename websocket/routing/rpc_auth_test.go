package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
)

type memoryWire struct {
	mu       sync.Mutex
	handlers map[string][]func([]byte)
	sent     map[string][][]byte
}

func newMemoryWire() *memoryWire {
	return &memoryWire{handlers: map[string][]func([]byte){}, sent: map[string][][]byte{}}
}

func (w *memoryWire) Ready() bool { return true }
func (w *memoryWire) Subscribe(_ context.Context, channel string, h func([]byte)) (func(), error) {
	w.mu.Lock()
	w.handlers[channel] = append(w.handlers[channel], h)
	w.mu.Unlock()
	return func() {}, nil
}
func (w *memoryWire) Publish(_ context.Context, channel string, b []byte) error {
	w.mu.Lock()
	w.sent[channel] = append(w.sent[channel], append([]byte(nil), b...))
	handlers := append([]func([]byte){}, w.handlers[channel]...)
	w.mu.Unlock()
	for _, h := range handlers {
		h(b)
	}
	return nil
}

// inject delivers raw bytes to a channel as an outside publisher would.
func (w *memoryWire) inject(channel string, b []byte) {
	w.mu.Lock()
	handlers := append([]func([]byte){}, w.handlers[channel]...)
	w.mu.Unlock()
	for _, h := range handlers {
		h(b)
	}
}

func authFor(t *testing.T, key byte, mode pubsub.AuthMode) *pubsub.Authenticator {
	t.Helper()
	a, err := pubsub.NewAuthenticator("k", map[string][]byte{"k": bytes.Repeat([]byte{key}, 32)}, mode)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func startNode(t *testing.T, wire Wire, node string, auth MessageAuthenticator, handled *atomic.Int32) *RPCTransport {
	t.Helper()
	tr := NewRPCTransport(wire, "auth", WithMessageAuthenticator(auth))
	err := tr.Start(context.Background(), node, func(_ context.Context, c DeliveryCommand) DeliveryResult {
		handled.Add(1)
		return DeliveryResult{Outcomes: []Outcome{{Location: Location{NodeID: node}, Status: Enqueued}}}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Close(context.Background()) })
	return tr
}

func deliver(tr *RPCTransport, target string) (DeliveryResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	return tr.Deliver(ctx, target, DeliveryCommand{Version: 1, MessageID: "m", TenantID: "t", TargetNode: target, Payload: []byte(`"x"`), ExpiresAt: time.Now().Add(time.Second)})
}

func TestRPCAuthenticatedRoundTrip(t *testing.T) {
	wire := newMemoryWire()
	var handledA, handledB atomic.Int32
	key := authFor(t, 1, pubsub.AuthRequired)
	a := startNode(t, wire, "node-a", key, &handledA)
	startNode(t, wire, "node-b", key, &handledB)

	result, err := deliver(a, "node-b")
	if err != nil || len(result.Outcomes) != 1 || result.Outcomes[0].Status != Enqueued {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if handledB.Load() != 1 {
		t.Fatalf("handled = %d", handledB.Load())
	}
}

func TestRPCRejectsCommandInjectedWithoutValidSignature(t *testing.T) {
	wire := newMemoryWire()
	var handled atomic.Int32
	tr := startNode(t, wire, "node-b", authFor(t, 1, pubsub.AuthRequired), &handled)

	forged, _ := json.Marshal(rpcEnvelope{Reply: "attacker", Command: &DeliveryCommand{Version: 1, MessageID: "evil", TenantID: "t", TargetNode: "node-b", Payload: []byte(`"owned"`), ExpiresAt: time.Now().Add(time.Second)}})
	wire.inject(tr.channel("delivery", "node-b"), forged)

	wrongKey, _ := authFor(t, 9, pubsub.AuthRequired).Seal(forged)
	wire.inject(tr.channel("delivery", "node-b"), wrongKey)

	if handled.Load() != 0 {
		t.Fatalf("forged command reached the gateway handler %d time(s)", handled.Load())
	}
	if len(wire.sent[tr.channel("result", "attacker")]) != 0 {
		t.Fatal("transport replied to a forged command")
	}
}

func TestRPCRejectsForgedResult(t *testing.T) {
	wire := newMemoryWire()
	var handled atomic.Int32
	key := authFor(t, 1, pubsub.AuthRequired)
	a := startNode(t, wire, "node-a", key, &handled)

	forge := func(corr string) {
		body, _ := json.Marshal(rpcEnvelope{CorrelationID: corr, Result: DeliveryResult{Outcomes: []Outcome{{Status: Enqueued}}}})
		wire.inject(a.channel("result", "node-a"), body)
	}
	done := make(chan DeliveryResult, 1)
	go func() {
		r, _ := deliver(a, "ghost")
		done <- r
	}()
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		for corr := range a.pending {
			a.mu.Unlock()
			forge(corr)
			a.mu.Lock()
		}
		a.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	if r := <-done; len(r.Outcomes) != 0 {
		t.Fatalf("forged result was accepted: %+v", r)
	}
}

// TestRPCRollingUpgradeStepsKeepClusterDelivering walks the documented rollout:
// each adjacent pair of steps must interoperate in both directions, otherwise a
// mixed-version cluster would drop deliveries mid-deploy.
func TestRPCRollingUpgradeStepsKeepClusterDelivering(t *testing.T) {
	modes := map[string]func(*testing.T) MessageAuthenticator{
		"none":     func(*testing.T) MessageAuthenticator { return nil },
		"passive":  func(t *testing.T) MessageAuthenticator { return authFor(t, 1, pubsub.AuthPassive) },
		"optional": func(t *testing.T) MessageAuthenticator { return authFor(t, 1, pubsub.AuthOptional) },
		"required": func(t *testing.T) MessageAuthenticator { return authFor(t, 1, pubsub.AuthRequired) },
	}
	pairs := [][2]string{{"none", "passive"}, {"passive", "optional"}, {"optional", "required"}, {"passive", "passive"}}
	for _, pair := range pairs {
		for _, direction := range [][2]string{{pair[0], pair[1]}, {pair[1], pair[0]}} {
			t.Run(direction[0]+"->"+direction[1], func(t *testing.T) {
				wire := newMemoryWire()
				var handledA, handledB atomic.Int32
				a := startNode(t, wire, "node-a", modes[direction[0]](t), &handledA)
				startNode(t, wire, "node-b", modes[direction[1]](t), &handledB)
				result, err := deliver(a, "node-b")
				if err != nil || len(result.Outcomes) != 1 || result.Outcomes[0].Status != Enqueued {
					t.Fatalf("mixed-version delivery failed: %+v %v", result, err)
				}
			})
		}
	}
}

func TestRPCRequiredRejectsUnsignedPeer(t *testing.T) {
	wire := newMemoryWire()
	var handledA, handledB atomic.Int32
	a := startNode(t, wire, "node-a", nil, &handledA)
	startNode(t, wire, "node-b", authFor(t, 1, pubsub.AuthRequired), &handledB)
	if _, err := deliver(a, "node-b"); err == nil || handledB.Load() != 0 {
		t.Fatalf("required node accepted an unsigned command: err=%v handled=%d", err, handledB.Load())
	}
}

package routing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/websocket/internal/wsrecovery"
	"sync"
)

// Wire implementations borrow their broker client. Cancel must stop callbacks.
type Wire interface {
	Subscribe(context.Context, string, func([]byte)) (func(), error)
	Publish(context.Context, string, []byte) error
	Ready() bool
}
type rpcEnvelope struct {
	Reply         string           `json:"reply,omitempty"`
	Command       *DeliveryCommand `json:"command,omitempty"`
	CorrelationID string           `json:"correlation_id,omitempty"`
	Result        DeliveryResult   `json:"result"`
}
type RPCTransport struct {
	wire            Wire
	prefix          string
	startMu         sync.Mutex
	mu              sync.Mutex
	node            string
	handler         DeliveryHandler
	pending         map[string]chan DeliveryResult
	cancels         []func()
	ctx             context.Context
	cancel          context.CancelFunc
	closeDone       chan struct{}
	started, closed bool
}

func NewRPCTransport(w Wire, prefix string) *RPCTransport {
	if prefix == "" {
		prefix = "websocket"
	}
	return &RPCTransport{wire: w, prefix: prefix, pending: make(map[string]chan DeliveryResult), closeDone: make(chan struct{})}
}
func (t *RPCTransport) channel(kind, node string) string {
	h := sha256.Sum256([]byte(node))
	return t.prefix + "." + kind + "." + hex.EncodeToString(h[:])
}
func (t *RPCTransport) Start(ctx context.Context, node string, h DeliveryHandler) error {
	t.startMu.Lock()
	defer t.startMu.Unlock()
	t.mu.Lock()
	if t.started || t.closed || node == "" || t.wire == nil {
		t.mu.Unlock()
		return ErrInvalid
	}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	t.node = node
	t.handler = h
	t.mu.Unlock()
	var cancels []func()
	for _, kind := range []string{"delivery", "result"} {
		cancel, err := t.wire.Subscribe(ctx, t.channel(kind, node), t.receive)
		if err != nil {
			for _, c := range cancels {
				c()
			}
			t.cancel()
			return err
		}
		cancels = append(cancels, cancel)
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
		return ErrUnavailable
	}
	t.cancels = cancels
	t.started = true
	t.mu.Unlock()
	return nil
}
func (t *RPCTransport) Ready() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.started && !t.closed && t.wire.Ready()
}
func (t *RPCTransport) receive(b []byte) {
	if len(b) > 2<<20 {
		return
	}
	var e rpcEnvelope
	if json.Unmarshal(b, &e) != nil {
		return
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	ctx, h, node := t.ctx, t.handler, t.node
	if e.Command == nil {
		ch := t.pending[e.CorrelationID]
		t.mu.Unlock()
		if ch != nil {
			select {
			case ch <- e.Result:
			default:
			}
		}
		return
	}
	t.mu.Unlock()
	if h == nil || e.Command.TargetNode != node || e.Reply == "" {
		return
	}
	result := func() (result DeliveryResult) {
		result = DeliveryResult{Outcomes: []Outcome{{Location: Location{NodeID: node}, Status: Unavailable}}}
		defer panicrecovery.Catch(wsrecovery.Handler(wsrecovery.Metadata{Component: "routing.delivery", NodeID: node}))
		return h(ctx, *e.Command)
	}()
	reply, _ := json.Marshal(rpcEnvelope{CorrelationID: e.Command.CorrelationID, Result: result})
	publishCtx, cancel := context.WithDeadline(ctx, e.Command.ExpiresAt)
	defer cancel()
	_ = t.wire.Publish(publishCtx, t.channel("result", e.Reply), reply)
}
func (t *RPCTransport) Deliver(ctx context.Context, node string, c DeliveryCommand) (DeliveryResult, error) {
	if c.CorrelationID == "" {
		c.CorrelationID = NewID()
	}
	ch := make(chan DeliveryResult, 1)
	t.mu.Lock()
	if !t.started || t.closed || len(t.pending) >= 1024 {
		t.mu.Unlock()
		return DeliveryResult{}, ErrUnavailable
	}
	if _, ok := t.pending[c.CorrelationID]; ok {
		t.mu.Unlock()
		return DeliveryResult{}, ErrInvalid
	}
	t.pending[c.CorrelationID] = ch
	reply := t.node
	stop := t.ctx.Done()
	t.mu.Unlock()
	defer func() { t.mu.Lock(); delete(t.pending, c.CorrelationID); t.mu.Unlock() }()
	b, err := json.Marshal(rpcEnvelope{Reply: reply, Command: &c})
	if err != nil {
		return DeliveryResult{}, err
	}
	if len(b) > 2<<20 {
		return DeliveryResult{}, ErrInvalid
	}
	if err = t.wire.Publish(ctx, t.channel("delivery", node), b); err != nil {
		return DeliveryResult{}, err
	}
	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return DeliveryResult{}, ctx.Err()
	case <-stop:
		return DeliveryResult{}, ErrUnavailable
	}
}
func (t *RPCTransport) Close(ctx context.Context) error {
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		if t.cancel != nil {
			t.cancel()
		}
		cs := t.cancels
		t.cancels = nil
		go func() {
			for _, cancel := range cs {
				cancel()
			}
			close(t.closeDone)
		}()
	}
	done := t.closeDone
	t.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

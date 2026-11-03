// Package nats provides targeted best-effort NATS Core delivery with gateway results.
package nats

import (
	"context"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
	"sync"
	"time"
)

type wire struct{ client *gonats.Conn }

func NewTransport(client *gonats.Conn, prefix string) *routing.RPCTransport {
	return routing.NewRPCTransport(&wire{client}, prefix)
}
func (w *wire) Ready() bool { return w.client != nil && w.client.IsConnected() }
func (w *wire) Publish(ctx context.Context, subject string, b []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return w.client.Publish(subject, b)
}
func (w *wire) Subscribe(ctx context.Context, subject string, handle func([]byte)) (func(), error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var mu sync.Mutex
	var callbacks sync.WaitGroup
	accepting := true
	sub, err := w.client.Subscribe(subject, func(m *gonats.Msg) {
		mu.Lock()
		if !accepting {
			mu.Unlock()
			return
		}
		callbacks.Add(1)
		mu.Unlock()
		defer callbacks.Done()
		handle(m.Data)
	})
	if err != nil {
		return nil, err
	}
	var once sync.Once
	stop := func() {
		once.Do(func() { mu.Lock(); accepting = false; mu.Unlock(); _ = sub.Unsubscribe() })
		callbacks.Wait()
	}
	if err = w.client.FlushWithContext(ctx); err != nil {
		stop()
		return nil, err
	}
	return stop, nil
}

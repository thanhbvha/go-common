// Package nats provides targeted best-effort NATS Core delivery with gateway results.
package nats

import (
	"context"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/websocket/routing"
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
	sub, err := w.client.Subscribe(subject, func(m *gonats.Msg) { handle(m.Data) })
	if err != nil {
		return nil, err
	}
	if err = w.client.FlushWithContext(ctx); err != nil {
		_ = sub.Unsubscribe()
		return nil, err
	}
	return func() { _ = sub.Unsubscribe() }, nil
}

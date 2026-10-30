package redis

import (
	"context"
	"sync"
	"sync/atomic"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/websocket/routing"
)

type wire struct {
	client        goredis.UniversalClient
	subscriptions atomic.Int32
}

func NewTransport(client goredis.UniversalClient, prefix string) *routing.RPCTransport {
	return routing.NewRPCTransport(&wire{client: client}, prefix)
}
func (w *wire) Ready() bool { return w.subscriptions.Load() == 2 }
func (w *wire) Publish(ctx context.Context, channel string, b []byte) error {
	return w.client.Publish(ctx, channel, b).Err()
}
func (w *wire) Subscribe(ctx context.Context, channel string, handle func([]byte)) (func(), error) {
	sub := w.client.Subscribe(ctx, channel)
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, err
	}
	w.subscriptions.Add(1)
	done := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(done)
		defer w.subscriptions.Add(-1)
		for msg := range sub.Channel() {
			handle([]byte(msg.Payload))
		}
	}()
	return func() { once.Do(func() { _ = sub.Close(); <-done }) }, nil
}

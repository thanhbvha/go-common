//go:build ignore

// Example 03: JetStream Push Subscribe with competing consumers (consumer group).
//
// Demonstrates:
//   - JSQueueSubscribe: multiple workers sharing a durable consumer (≈ Redis XREADGROUP multi-consumer)
//   - JSSubscribe: push-based individual consumer
//   - Manual ACK with WithManualAck()
//
// Run:
//
//	docker run -d --name nats -p 4222:4222 nats:latest -js
//	go run examples/nats/03_consumer_group/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/nats"
)

func main() {
	l := logger.New(logger.DefaultOptions())
	logger.SetDefault(l)
	defer logger.Close()

	ctx := context.Background()
	cfg := nats.DefaultConfig()
	cfg.Logger = l
	client := nats.MustConnect(ctx, cfg)
	defer client.Close()
	l.Info("✅ Connected")

	// Create stream
	_ = client.AddStream(ctx, "NOTIFICATIONS", []string{"notify.>"},
		nats.WithRetention(nats.WorkQueuePolicy),
	)

	// ── Push queue subscribe: 3 competing workers ───────────────────────
	// Only one worker receives each message (load-balanced).
	// Equivalent to multiple consumers in the same XREADGROUP.
	var wg sync.WaitGroup
	received := make([]string, 0)
	var mu sync.Mutex

	makeWorker := func(id int) nats.MsgHandler {
		return func(msg *nats.Msg) {
			mu.Lock()
			received = append(received, fmt.Sprintf("worker-%d:seq=%d", id, msg.Sequence))
			mu.Unlock()
			l.Info("worker got message", "worker_id", id, "data", string(msg.Data), "seq", msg.Sequence)
			time.Sleep(10 * time.Millisecond) // simulate processing
			_ = msg.Ack()
			wg.Done()
		}
	}

	sub1, err := client.JSQueueSubscribe("notify.>", "notify-group", "notify-durable",
		makeWorker(1), nats.WithManualAck())
	if err != nil {
		l.Error("JSQueueSubscribe worker 1 failed", "err", err)
		os.Exit(1)
	}
	sub2, err := client.JSQueueSubscribe("notify.>", "notify-group", "notify-durable",
		makeWorker(2), nats.WithManualAck())
	if err != nil {
		l.Error("JSQueueSubscribe worker 2 failed", "err", err)
		os.Exit(1)
	}
	sub3, err := client.JSQueueSubscribe("notify.>", "notify-group", "notify-durable",
		makeWorker(3), nats.WithManualAck())
	if err != nil {
		l.Error("JSQueueSubscribe worker 3 failed", "err", err)
		os.Exit(1)
	}
	defer sub1.Unsubscribe()
	defer sub2.Unsubscribe()
	defer sub3.Unsubscribe()

	// Publish 6 messages
	wg.Add(6)
	for i := 1; i <= 6; i++ {
		_, _ = client.JSPublish(ctx, fmt.Sprintf("notify.event-%d", i),
			[]byte(fmt.Sprintf("event-%d", i)))
	}

	wg.Wait()
	l.Info("\n📊 Distribution across workers:")
	for _, r := range received {
		l.Info("  received", "worker_details", r)
	}

	// ── List consumers  (≈ XINFO GROUPS) ───────────────────────────────
	consumers, _ := client.ListConsumers(ctx, "NOTIFICATIONS")
	l.Info("\n👥 Consumers on NOTIFICATIONS:")
	for _, c := range consumers {
		l.Info("  consumer", "durable", c.Durable, "pending", c.NumPending, "ackPending", c.NumAckPending)
	}
}

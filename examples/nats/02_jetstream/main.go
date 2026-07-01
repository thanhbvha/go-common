//go:build ignore

// Example 02: JetStream – AddStream, JSPublish, PullSubscribe, Fetch, Ack/Nak/Term.
//
// Demonstrates the full Redis-Stream-equivalent workflow:
//
//	XAdd  → JSPublish
//	XGroupCreateMkStream → AddStream + AddConsumer
//	XReadGroup (pull) → PullSubscribe + Fetch
//	XAck  → msg.Ack()
//	XNack → msg.Nak() / NakWithDelay()
//	discard → msg.Term()
//
// Run with JetStream-enabled NATS:
//
//	docker run -d --name nats -p 4222:4222 nats:latest -js
//	go run examples/nats/02_jetstream/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/nats"
)

const (
	streamName = "ORDERS"
	subject    = "orders.new"
	durable    = "order-processor"
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
	l.Info("✅ Connected to NATS JetStream")

	// ── 1. Create stream  (≈ XGROUP CREATE orders $ MKSTREAM) ──────────
	err := client.AddStream(ctx, streamName, []string{subject},
		nats.WithRetention(nats.WorkQueuePolicy), // remove on ACK
		nats.WithMaxMsgs(10_000),
		nats.WithMaxAge(24*time.Hour),
	)
	if err != nil {
		l.Error("AddStream failed", "err", err)
		os.Exit(1)
	}
	l.Info("📦 Stream created", "stream", streamName)

	// ── 2. Create durable consumer  (≈ XGROUP CREATE) ──────────────────
	err = client.AddConsumer(ctx, streamName, durable,
		nats.WithAckWait(30*time.Second),
		nats.WithMaxDeliver(3), // 3 retries then dead-letter
	)
	if err != nil {
		l.Error("AddConsumer failed", "err", err)
		os.Exit(1)
	}
	l.Info("👤 Consumer created", "consumer", durable)

	// ── 3. Publish messages  (≈ XADD) ──────────────────────────────────
	type Order struct {
		ID     string `json:"id"`
		Amount int    `json:"amount"`
	}
	for i := 1; i <= 5; i++ {
		ack, err := client.JSPublishJSON(subject, Order{
			ID:     fmt.Sprintf("ORD-%03d", i),
			Amount: i * 100,
		})
		if err != nil {
			l.Error("JSPublishJSON failed", "err", err)
			os.Exit(1)
		}
		l.Info("  📨 published", "seq", ack.Sequence)
	}

	// ── 4. Pull subscribe  (≈ XREADGROUP with pull / BLOCK) ────────────
	ps, err := client.PullSubscribe(subject, durable)
	if err != nil {
		l.Error("PullSubscribe failed", "err", err)
		os.Exit(1)
	}
	defer ps.Unsubscribe()

	l.Info("\n📥 Fetching messages", "batch", 3)
	msgs, err := client.Fetch(ps, 3, nats.WithFetchTimeout(5*time.Second))
	if err != nil {
		l.Error("Fetch failed", "err", err)
		os.Exit(1)
	}

	for i, msg := range msgs {
		var ord Order
		if err := msg.DecodeJSON(&ord); err != nil {
			l.Error("DecodeJSON failed", "err", err)
			os.Exit(1)
		}

		switch {
		case i == 0:
			// Normal ACK  (≈ XACK)
			l.Info("  ✅ ACK", "seq", msg.Sequence, "id", ord.ID)
			_ = msg.Ack()

		case i == 1:
			// NakWithDelay – redeliver after 2s (≈ XCLAIM defer)
			l.Info("  ⏳ NAK-DELAY", "seq", msg.Sequence, "id", ord.ID, "retry_in", "2s")
			_ = msg.NakWithDelay(2 * time.Second)

		case i == 2:
			// Term – discard permanently (≈ dead-letter after MaxDeliver)
			l.Info("  ☠️  TERM", "seq", msg.Sequence, "id", ord.ID)
			_ = msg.Term()
		}
	}

	// ── 5. Stream & Consumer info  (≈ XLEN / XPENDING / XINFO GROUPS) ──
	info, _ := client.GetStreamInfo(ctx, streamName)
	l.Info("\n📊 Stream info", "msgs", info.Msgs, "firstSeq", info.FirstSeq, "lastSeq", info.LastSeq)

	consumerInfo, _ := client.GetConsumerInfo(ctx, streamName, durable)
	l.Info("👥 Consumer info", "pending", consumerInfo.NumPending, "ackPending", consumerInfo.NumAckPending, "redelivered", consumerInfo.NumRedelivered)

	// ── 6. Remaining messages ───────────────────────────────────────────
	l.Info("\n📥 Fetching remaining (no-wait)")
	remaining, _ := client.FetchNoWait(ps, 10)
	for _, msg := range remaining {
		var ord Order
		_ = msg.DecodeJSON(&ord)
		l.Info("  📦 fetched", "seq", msg.Sequence, "id", ord.ID, "deliveries", msg.NumDelivered)
		_ = msg.Ack()
	}
	l.Info("done", "remaining_messages", len(remaining))
}

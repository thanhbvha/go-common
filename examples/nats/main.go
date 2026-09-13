package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/nats"
)

func main() {
	// Initialize logger
	l := logger.New(logger.DefaultOptions())
	logger.SetDefault(l)
	defer logger.Close()

	fmt.Println("=== NATS & JetStream Module Examples ===")
	fmt.Println("Requires a local NATS JetStream server running:")
	fmt.Println("  docker run -d --name nats -p 4222:4222 nats:latest -js")
	fmt.Println("---------------------------------------------------------")

	// Initialize NATS client
	ctx := context.Background()
	cfg := nats.DefaultConfig()
	cfg.Logger = l
	
	client := nats.MustConnect(ctx, cfg)
	
	// CRITICAL: Always defer client.Close() to safely drain active subscriptions
	// and flush pending outgoing messages to the server before the application exits.
	defer client.Close()
	
	l.Info("✅ Connected to NATS & JetStream")

	// Uncomment the example you want to run:
	RunPubSubExample(client)
	// RunJetStreamExample(client)
	// RunConsumerGroupExample(client)
	// RunKVStoreExample(client)
	// RunJSONHelpersExample(client)
}

// =====================================================================
// 1. NATS Core Pub/Sub, Queue Subscribe, and Request/Reply.
// =====================================================================
func RunPubSubExample(client *nats.Client) {
	fmt.Println("\n--- 1. Pub/Sub Example ---")
	l := logger.Default()

	// ── 1. Simple Subscribe + Publish ──────────────────────────────────
	sub, err := client.Subscribe("demo.greet", func(msg *gonats.Msg) {
		l.Info("[sub] received", "subject", msg.Subject, "data", string(msg.Data))
	})
	if err != nil {
		l.Error("Subscribe failed", "err", err)
		os.Exit(1)
	}
	defer client.Unsubscribe(sub)

	if err := client.Publish("demo.greet", []byte("hello world")); err != nil {
		l.Error("Publish failed", "err", err)
		os.Exit(1)
	}
	time.Sleep(100 * time.Millisecond) // let the goroutine print

	// ── 2. PublishJSON ──────────────────────────────────────────────────
	type Greeting struct {
		From    string `json:"from"`
		Message string `json:"message"`
	}

	jsonSub, _ := client.Subscribe("demo.json", func(msg *gonats.Msg) {
		l.Info("[json-sub] raw payload", "data", string(msg.Data))
	})
	defer client.Unsubscribe(jsonSub)

	_ = client.PublishJSON("demo.json", Greeting{From: "go-common", Message: "Xin chao"})
	time.Sleep(100 * time.Millisecond)

	// ── 3. Queue Subscribe (competing consumers) ────────────────────────
	handler := func(msg *gonats.Msg) {
		l.Info("[queue] worker received", "data", string(msg.Data))
	}
	q1, _ := client.QueueSubscribe("demo.tasks", "workers", handler)
	q2, _ := client.QueueSubscribe("demo.tasks", "workers", handler)
	defer q1.Unsubscribe()
	defer q2.Unsubscribe()

	for i := 1; i <= 4; i++ {
		_ = client.Publish("demo.tasks", []byte(fmt.Sprintf("task-%d", i)))
	}
	time.Sleep(200 * time.Millisecond)

	// ── 4. Request / Reply ──────────────────────────────────────────────
	replySub, _ := client.Subscribe("demo.ping", func(msg *gonats.Msg) {
		_ = msg.Respond([]byte("pong"))
	})
	defer replySub.Unsubscribe()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reply, err := client.Request(ctx, "demo.ping", []byte("ping"))
	if err != nil {
		l.Error("Request failed", "err", err)
		os.Exit(1)
	}
	l.Info("[request-reply] got", "data", string(reply.Data))
}

// =====================================================================
// 2. JetStream – AddStream, JSPublish, PullSubscribe, Fetch, Ack/Nak/Term.
// =====================================================================
func RunJetStreamExample(client *nats.Client) {
	fmt.Println("\n--- 2. JetStream Example ---")
	l := logger.Default()
	ctx := context.Background()

	const (
		streamName = "ORDERS"
		subject    = "orders.new"
		durable    = "order-processor"
	)

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
	defer client.DeleteStream(ctx, streamName)
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

// =====================================================================
// 3. JetStream Push Subscribe with competing consumers (consumer group).
// =====================================================================
func RunConsumerGroupExample(client *nats.Client) {
	fmt.Println("\n--- 3. Consumer Group Example ---")
	l := logger.Default()
	ctx := context.Background()

	// Create stream
	_ = client.AddStream(ctx, "NOTIFICATIONS", []string{"notify.>"},
		nats.WithRetention(nats.WorkQueuePolicy),
	)
	defer client.DeleteStream(ctx, "NOTIFICATIONS")

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

// =====================================================================
// 4. KV Store Example
// =====================================================================
func RunKVStoreExample(client *nats.Client) {
	fmt.Println("\n--- 4. KV Store Example ---")
	l := logger.Default()
	ctx := context.Background()
	bucket := "feature-flags"

	// ── 1. Create KV bucket with History=5 ─────────────────────────────
	err := client.KVCreate(ctx, bucket,
		nats.WithKVHistory(5),                 // keep last 5 revisions
		nats.WithKVTTL(10*time.Minute),        // auto-expire entries after 10m
		nats.WithKVDescription("Feature flag store"),
	)
	if err != nil {
		l.Error("KVCreate failed", "err", err)
		os.Exit(1)
	}
	l.Info("🗄️  KV bucket created", "bucket", bucket)

	// ── 2. Put values ───────────────────────────────────────────────────
	rev1, _ := client.KVPut(ctx, bucket, "dark-mode", []byte("false"))
	l.Info("  PUT dark-mode=false", "rev", rev1)

	rev2, _ := client.KVPut(ctx, bucket, "dark-mode", []byte("true"))
	l.Info("  PUT dark-mode=true", "rev", rev2)

	_, _ = client.KVPut(ctx, bucket, "beta-ui", []byte("false"))

	// ── 3. Get ─────────────────────────────────────────────────────────
	entry, err := client.KVGet(ctx, bucket, "dark-mode")
	if err != nil {
		l.Error("KVGet failed", "err", err)
		os.Exit(1)
	}
	l.Info("\n  GET dark-mode", "value", string(entry.Value), "rev", entry.Revision, "op", entry.Operation)

	// ── 4. KVCreate2 – set only if not exists (≈ Redis SET NX) ─────────
	_, err = client.KVCreate2(ctx, bucket, "dark-mode", []byte("force"))
	l.Info("  CREATE2 dark-mode (expect error)", "err", err) // should fail

	_, err = client.KVCreate2(ctx, bucket, "new-flag", []byte("enabled"))
	l.Info("  CREATE2 new-flag", "err", err) // should succeed

	// ── 5. KVUpdate with optimistic locking ────────────────────────────
	current, _ := client.KVGet(ctx, bucket, "dark-mode")
	rev3, err := client.KVUpdate(ctx, bucket, "dark-mode", []byte("false"), current.Revision)
	if err != nil {
		l.Info("  UPDATE conflict", "err", err)
	} else {
		l.Info("  UPDATE dark-mode=false", "rev", rev3)
	}

	// Simulate concurrent write conflict:
	_, _ = client.KVPut(ctx, bucket, "dark-mode", []byte("true")) // bumps revision
	_, err = client.KVUpdate(ctx, bucket, "dark-mode", []byte("false"), rev3)
	l.Info("  UPDATE with stale revision (expect error)", "err", err)

	// ── 6. History ──────────────────────────────────────────────────────
	history, _ := client.KVHistory(ctx, bucket, "dark-mode")
	l.Info("\n📜 History for dark-mode", "revisions", len(history))
	for _, h := range history {
		l.Info("  history", "rev", h.Revision, "op", h.Operation, "value", string(h.Value))
	}

	// ── 7. Watch a key ──────────────────────────────────────────────────
	watcher, err := client.KVWatch(ctx, bucket, "dark-mode")
	if err != nil {
		l.Error("KVWatch failed", "err", err)
		os.Exit(1)
	}
	defer watcher.Stop()

	// Trigger changes in background
	go func() {
		time.Sleep(100 * time.Millisecond)
		_, _ = client.KVPut(ctx, bucket, "dark-mode", []byte("true"))
		time.Sleep(100 * time.Millisecond)
		_, _ = client.KVPut(ctx, bucket, "dark-mode", []byte("false"))
		time.Sleep(100 * time.Millisecond)
		_ = client.KVDeleteKey(ctx, bucket, "dark-mode")
	}()

	l.Info("\n👀 Watching dark-mode (3 updates)")
	for i := 0; i < 3; i++ {
		update := <-watcher.Updates()
		if update == nil {
			break
		}
		switch update.Operation() {
		case gonats.KeyValuePut:
			l.Info("  CHANGED dark-mode", "value", string(update.Value()), "rev", update.Revision())
		case gonats.KeyValueDelete:
			l.Info("  DELETED dark-mode", "rev", update.Revision())
		}
	}

	// ── 8. List all keys ────────────────────────────────────────────────
	keys, _ := client.KVKeys(ctx, bucket)
	l.Info("\n🔑 Keys in bucket", "bucket", bucket, "keys", keys)

	// ── 9. Cleanup ──────────────────────────────────────────────────────
	_ = client.KVPurgeKey(ctx, bucket, "dark-mode") // delete + wipe history
	_ = client.KVDeleteBucket(ctx, bucket)
	l.Info("\n🧹 Bucket deleted")
}

// =====================================================================
// 5. JSON Helpers Example
// =====================================================================

type UserEvent struct {
	UserID string    `json:"user_id"`
	Action string    `json:"action"`
	At     time.Time `json:"at"`
}

type AppConfig struct {
	Theme    string `json:"theme"`
	Language string `json:"language"`
	Debug    bool   `json:"debug"`
}

func RunJSONHelpersExample(client *nats.Client) {
	fmt.Println("\n--- 5. JSON Helpers Example ---")
	l := logger.Default()
	ctx := context.Background()

	// ── 1. JSPublishJSON + DecodeJSON (JetStream) ───────────────────────
	_ = client.AddStream(ctx, "EVENTS", []string{"events.>"})

	ack, err := client.JSPublishJSON("events.user", UserEvent{
		UserID: "u-001",
		Action: "login",
		At:     time.Now(),
	})
	if err != nil {
		l.Error("JSPublishJSON failed", "err", err)
		os.Exit(1)
	}
	l.Info("📨 JSPublishJSON", "stream", ack.Stream, "seq", ack.Sequence)

	_ = client.AddConsumer(ctx, "EVENTS", "event-reader")
	ps, _ := client.PullSubscribe("events.>", "event-reader")
	defer ps.Unsubscribe()

	msgs, _ := client.Fetch(ps, 1)
	if len(msgs) > 0 {
		var evt UserEvent
		if err := msgs[0].DecodeJSON(&evt); err != nil {
			l.Error("DecodeJSON failed", "err", err)
			os.Exit(1)
		}
		l.Info("📥 DecodeJSON", "user", evt.UserID, "action", evt.Action)
		_ = msgs[0].Ack()
	}

	// ── 2. PublishJSON (Core Pub/Sub) ───────────────────────────────────
	sub, _ := client.Subscribe("config.update", func(msg *gonats.Msg) {
		l.Info("🔔 Core sub received", "data", string(msg.Data))
	})
	defer client.Unsubscribe(sub)

	_ = client.PublishJSON("config.update", AppConfig{
		Theme:    "dark",
		Language: "vi",
		Debug:    true,
	})
	time.Sleep(100 * time.Millisecond)

	// ── 3. KVPutJSON + KVGetJSON ────────────────────────────────────────
	_ = client.KVCreate(ctx, "app-config", nats.WithKVHistory(3))

	rev, err := client.KVPutJSON("app-config", "ui", AppConfig{
		Theme:    "dark",
		Language: "en",
		Debug:    false,
	})
	if err != nil {
		l.Error("KVPutJSON failed", "err", err)
		os.Exit(1)
	}
	l.Info("🗄️  KVPutJSON", "rev", rev)

	var appCfg AppConfig
	if err := client.KVGetJSON("app-config", "ui", &appCfg); err != nil {
		l.Error("KVGetJSON failed", "err", err)
		os.Exit(1)
	}
	l.Info("📖 KVGetJSON", "theme", appCfg.Theme, "lang", appCfg.Language, "debug", appCfg.Debug)

	// Cleanup
	_ = client.KVDeleteBucket(ctx, "app-config")
	_ = client.DeleteStream(ctx, "EVENTS")
	l.Info("✅ Done")
}

//go:build ignore

// Example 05: JSON helpers – JSPublishJSON, DecodeJSON, PublishJSON, KVPutJSON/KVGetJSON.
//
// Run:
//
//	docker run -d --name nats -p 4222:4222 nats:latest -js
//	go run examples/nats/05_json_helpers/main.go
package main

import (
	"context"
	"os"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/nats"
)

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

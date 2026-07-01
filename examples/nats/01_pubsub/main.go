//go:build ignore

// Example 01: NATS Core Pub/Sub, Queue Subscribe, and Request/Reply.
//
// Run with a local NATS server:
//
//	docker run -d --name nats -p 4222:4222 nats:latest
//	go run examples/nats/01_pubsub/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/nats"
)

func main() {
	l := logger.New(logger.DefaultOptions())
	logger.SetDefault(l)
	defer logger.Close()

	cfg := nats.DefaultConfig()
	cfg.Logger = l
	client := nats.MustConnect(context.Background(), cfg)
	defer client.Close()
	l.Info("✅ Connected to NATS")

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

	_ = client.PublishJSON("demo.json", Greeting{From: "go-common", Message: "สวัสดี"})
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

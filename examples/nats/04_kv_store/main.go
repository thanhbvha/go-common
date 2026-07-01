//go:build ignore

// Example 04: NATS KV Store – Put/Get/Update/Delete, Watch, History, Optimistic Lock.
//
// Demonstrates:
//   - KVCreate with TTL and History options
//   - KVPut / KVGet / KVDeleteKey / KVPurgeKey
//   - KVUpdate with optimistic locking (revision check)
//   - KVCreate2 (set-if-not-exists ≈ Redis SET NX)
//   - KVWatch – real-time change notifications
//   - KVHistory – version history per key
//   - KVKeys – list all keys
//
// Run:
//
//	docker run -d --name nats -p 4222:4222 nats:latest -js
//	go run examples/nats/04_kv_store/main.go
package main

import (
	"context"
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

	ctx := context.Background()
	cfg := nats.DefaultConfig()
	cfg.Logger = l
	client := nats.MustConnect(ctx, cfg)
	defer client.Close()
	l.Info("✅ Connected")

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

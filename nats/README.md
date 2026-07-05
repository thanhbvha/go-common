# nats

NATS client wrapper with lifecycle management, JetStream, KV Store, and JSON helpers. API pattern mirrors the `redis` package.

### Prerequisites

```bash
# Start NATS with JetStream enabled
docker run -d --name nats -p 4222:4222 nats:latest -js
```

### Quick start

```go
import "github.com/thanhbvha/go-common/nats"

cfg := nats.DefaultConfig()
cfg.URLs   = []string{"nats://localhost:4222"}
cfg.Logger = log  // any nats.Logger-compatible value

client := nats.MustConnect(ctx, cfg)
nats.SetDefault(client)
defer nats.Close()
```

### Config reference

```go
cfg := nats.DefaultConfig()

// Connection
cfg.URLs           = []string{"nats://node1:4222", "nats://node2:4222"} // cluster
cfg.NKeyFile       = "/run/secrets/nats.nkey"   // NKey auth (optional)
cfg.CredFile       = "/run/secrets/app.creds"   // Creds auth (optional)
cfg.ConnectTimeout = 5 * time.Second
cfg.ReconnectWait  = 2 * time.Second
cfg.MaxReconnects  = 60   // -1 = unlimited

// JetStream defaults
cfg.DefaultStorage  = nats.MemoryStorage  // or nats.FileStorage
cfg.DefaultReplicas = 1                   // set > 1 for HA clusters
```

### Core Pub/Sub

```go
// Subscribe
sub, _ := client.Subscribe("events.user", func(msg *gonats.Msg) {
    fmt.Println("received:", string(msg.Data))
})
defer client.Unsubscribe(sub)

// Publish
_ = client.Publish("events.user", []byte(`{"action":"login"}`))

// JSON shorthand
_ = client.PublishJSON("events.user", map[string]any{"action": "login"})

// Queue subscribe (competing consumers / load-balanced)
q, _ := client.QueueSubscribe("tasks.>", "worker-pool", handler)

// Request/Reply
reply, _ := client.Request(ctx, "rpc.ping", []byte("ping"))
```

### JetStream – Stream & Consumer management

```go
// Create stream  (≈ XGROUP CREATE ... MKSTREAM)
_ = client.AddStream(ctx, "ORDERS", []string{"orders.>"},
    nats.WithRetention(nats.WorkQueuePolicy),  // delete on ACK
    nats.WithMaxMsgs(1_000_000),
    nats.WithMaxAge(7 * 24 * time.Hour),
    nats.WithStorage(nats.FileStorage),        // override default
)

// Create durable consumer  (≈ XGROUP CREATE)
_ = client.AddConsumer(ctx, "ORDERS", "order-svc",
    nats.WithAckWait(30 * time.Second),
    nats.WithMaxDeliver(5),
)

// Stream info  (≈ XLEN / XINFO STREAM)
info, _ := client.GetStreamInfo(ctx, "ORDERS")
fmt.Println(info.Msgs, info.LastSeq)

// Consumer info  (≈ XPENDING / XINFO GROUPS)
ci, _ := client.GetConsumerInfo(ctx, "ORDERS", "order-svc")
fmt.Println(ci.NumPending, ci.NumAckPending)

// List consumers  (≈ XINFO CONSUMERS)
consumers, _ := client.ListConsumers(ctx, "ORDERS")
```

### JetStream – Publish  (≈ XADD)

```go
// Raw bytes
ack, _ := client.JSPublish(ctx, "orders.new", payload)
fmt.Println("seq:", ack.Sequence)

// JSON shorthand
ack, _ = client.JSPublishJSON("orders.new", order)

// Async (fire-and-check)
future, _ := client.JSPublishAsync("orders.new", payload)
select {
case ack := <-future.Ok():
    fmt.Println("ack seq:", ack.Sequence)
case err := <-future.Err():
    log.Println("publish failed:", err)
}
```

### JetStream – Pull Subscribe (≈ XREADGROUP pull / BLOCK)

```go
ps, _ := client.PullSubscribe("orders.>", "order-svc")
defer ps.Unsubscribe()

// Fetch up to 10, block for up to 5s
msgs, _ := client.Fetch(ps, 10, nats.WithFetchTimeout(5*time.Second))
for _, msg := range msgs {
    var order Order
    _ = msg.DecodeJSON(&order)  // JSON helper
    
    if err := process(order); err != nil {
        _ = msg.NakWithDelay(10 * time.Second) // retry after 10s  (≈ XCLAIM)
        continue
    }
    _ = msg.Ack()  // (≈ XACK)
}

// Non-blocking fetch
msgs, _ = client.FetchNoWait(ps, 10)
```

### JetStream – Push Subscribe (≈ XREADGROUP push mode)

```go
// Individual push consumer
sub, _ := client.JSSubscribe("orders.>", "order-svc",
    func(msg *nats.Msg) {
        _ = process(msg)
        _ = msg.Ack()
    },
    nats.WithManualAck(),
)

// Queue / competing consumers (multiple workers sharing one durable)
for i := 0; i < 3; i++ {
    workerID := i
    client.JSQueueSubscribe("orders.>", "order-workers", "order-svc",
        func(msg *nats.Msg) {
            fmt.Printf("worker %d processing seq=%d\n", workerID, msg.Sequence)
            _ = msg.Ack()
        },
        nats.WithManualAck(),
    )
}
```

### Message ACK / NAK

| Method | Equivalent | Description |
|---|---|---|
| `msg.Ack()` | `XACK` | Acknowledge – remove from pending |
| `msg.Nak()` | – | Redeliver immediately |
| `msg.NakWithDelay(d)` | `XCLAIM` | Redeliver after delay d |
| `msg.Term()` | Dead-letter | Discard permanently (MaxDeliver reached) |
| `msg.InProgress()` | – | Reset AckWait timer (long processing) |

### KV Store

```go
// Create bucket  (TTL = per bucket, History = revisions per key)
_ = client.KVCreate(ctx, "config",
    nats.WithKVHistory(5),
    nats.WithKVTTL(24 * time.Hour),
    nats.WithKVStorage(nats.FileStorage),
)

// CRUD
rev, _ := client.KVPut(ctx, "config", "theme", []byte("dark"))
entry, _ := client.KVGet(ctx, "config", "theme")
fmt.Println(string(entry.Value), "rev:", entry.Revision)

// JSON helpers
_, _ = client.KVPutJSON("config", "ui", AppConfig{Theme: "dark"})
var cfg AppConfig
_ = client.KVGetJSON("config", "ui", &cfg)

// Set only if not exists  (≈ Redis SET NX)
_, err := client.KVCreate2(ctx, "config", "theme", []byte("light"))
// → error if key already exists

// Optimistic locking
current, _ := client.KVGet(ctx, "config", "theme")
newRev, err := client.KVUpdate(ctx, "config", "theme", []byte("light"), current.Revision)
// → error if a concurrent write changed the revision

// Watch – real-time change notifications (no Redis equivalent, needs Pub/Sub manually)
watcher, _ := client.KVWatch(ctx, "config", "theme")
defer watcher.Stop()
for entry := range watcher.Updates() {
    if entry == nil { continue } // initial value sentinel
    fmt.Printf("changed: %s op=%v\n", string(entry.Value()), entry.Operation())
}

// History
history, _ := client.KVHistory(ctx, "config", "theme") // requires History > 1
for _, h := range history {
    fmt.Printf("rev=%d value=%s\n", h.Revision, string(h.Value))
}

// All keys
keys, _ := client.KVKeys(ctx, "config")

// Delete
_ = client.KVDeleteKey(ctx, "config", "theme")   // soft delete (history preserved)
_ = client.KVPurgeKey(ctx, "config", "theme")    // hard delete (wipes history)
_ = client.KVDeleteBucket(ctx, "config")         // drop entire bucket
```

### Stream options reference

| Option | Description |
|---|---|
| `WithRetention(r)` | `LimitsPolicy` \| `WorkQueuePolicy` \| `InterestPolicy` |
| `WithStorage(s)` | `MemoryStorage` (default) \| `FileStorage` |
| `WithMaxMsgs(n)` | Max messages before oldest is removed |
| `WithMaxBytes(n)` | Max total bytes |
| `WithMaxAge(d)` | Max message age |
| `WithReplicas(n)` | Replication factor (requires cluster) |
| `WithDuplicateWindow(d)` | Dedup window using `Nats-Msg-Id` header |

### Consumer options reference

| Option | Description |
|---|---|
| `WithAckWait(d)` | Redelivery timeout (≈ XClaim idle threshold) |
| `WithMaxDeliver(n)` | Max delivery attempts (-1 = unlimited) |
| `WithMaxAckPending(n)` | Max in-flight messages |
| `WithDeliverPolicy(p)` | `DeliverAll` \| `DeliverLast` \| `DeliverNew` \| `DeliverByStartSeq` |
| `WithStartSequence(seq)` | Start from sequence (≈ XRANGE start) |
| `WithFilterSubject(s)` | Filter to specific subject within stream |

### Key types

| Symbol | Description |
|---|---|
| `Config` | All connection and JetStream default options |
| `StorageType` | `MemoryStorage` \| `FileStorage` |
| `RetentionPolicy` | `LimitsPolicy` \| `WorkQueuePolicy` \| `InterestPolicy` |
| `Client` | Thread-safe wrapper — Core + JetStream + KV |
| `New(cfg)` | Allocate (does not connect) |
| `MustConnect(ctx, cfg)` | Allocate + connect, panic on error |
| `SetDefault(c)` / `Default()` | Process-wide singleton |
| `StreamInfo` | Simplified JetStream stream state |
| `ConsumerInfo` | Simplified consumer state (NumPending ≈ XPENDING) |
| `Msg` | Wrapped message with `.Ack/.Nak/.NakWithDelay/.Term/.InProgress` |
| `PullSubscription` | Handle for pull-based fetch |
| `PubAck` | Publish confirmation (Stream, Sequence, Duplicate) |
| `KVEntry` | KV record (Key, Value, Revision, Operation) |

// Package eventbus supplies the same real-broker fault suite for every
// transactional messaging database driver. It is test support, not runtime code.
package eventbus

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/v2/db/messaging"
	consumer "github.com/thanhbvha/go-common/v2/db/messaging/consumer/eventbus"
	"github.com/thanhbvha/go-common/v2/db/messaging/contracttest"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	publisher "github.com/thanhbvha/go-common/v2/db/messaging/publisher/eventbus"
	bus "github.com/thanhbvha/go-common/v2/eventbus"
	kafkadriver "github.com/thanhbvha/go-common/v2/eventbus/driver/kafka"
	natsdriver "github.com/thanhbvha/go-common/v2/eventbus/driver/nats"
	redisdriver "github.com/thanhbvha/go-common/v2/eventbus/driver/redis"
	"github.com/thanhbvha/go-common/v2/eventbus/transport"
	commonkafka "github.com/thanhbvha/go-common/v2/kafka"
)

func broker(t *testing.T, name string) transport.Transport {
	t.Helper()
	switch name {
	case "nats":
		s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
		if err != nil {
			t.Fatal(err)
		}
		s.Start()
		t.Cleanup(s.Shutdown)
		if !s.ReadyForConnections(5 * time.Second) {
			t.Fatal("NATS not ready")
		}
		nc, err := gonats.Connect(s.ClientURL())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		js, err := nc.JetStream()
		if err != nil {
			t.Fatal(err)
		}
		return natsdriver.New(js)
	case "redis":
		addr := os.Getenv("EVENTBUS_REDIS_ADDR")
		if addr == "" {
			if os.Getenv("MESSAGING_BROKERS_REQUIRED") == "1" {
				t.Fatal("EVENTBUS_REDIS_ADDR required")
			}
			t.Skip("set isolated Redis fixture address")
		}
		c := goredis.NewClient(&goredis.Options{Addr: addr})
		t.Cleanup(func() { _ = c.Close() })
		return redisdriver.New(c)
	case "kafka":
		addr := os.Getenv("EVENTBUS_KAFKA_BROKERS")
		if addr == "" {
			if os.Getenv("MESSAGING_BROKERS_REQUIRED") == "1" {
				t.Fatal("EVENTBUS_KAFKA_BROKERS required")
			}
			t.Skip("set isolated Kafka fixture address")
		}
		cfg := commonkafka.DefaultConfig()
		cfg.Brokers = strings.Split(addr, ",")
		cfg.ProducerAcks = -1
		tr, err := kafkadriver.NewWithConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return tr
	}
	t.Fatal("unknown fixture")
	return nil
}

type publicationFault struct {
	publisher outbox.Publisher
	drop      atomic.Bool
}

func (p *publicationFault) Publish(ctx context.Context, m messaging.Message) error {
	if err := p.publisher.Publish(ctx, m); err != nil {
		return err
	}
	if p.drop.CompareAndSwap(true, false) {
		return context.DeadlineExceeded
	}
	return nil
}

// TestPostgresEventBus exercises real broker acceptance followed by response
// loss, expired claims, handler retry/DLQ replay and independently scoped tenants.
func Run[T any](t *testing.T, fixture func(*testing.T) contracttest.Fixture[T], names ...string) {
	if len(names) == 0 {
		names = []string{"nats", "redis", "kafka"}
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			tr := broker(t, name)
			f := fixture(t)
			ctx := t.Context()
			cfg := bus.DefaultConfig()
			cfg.Namespace = f.Namespace
			cfg.ProcessTimeout = 3 * time.Second
			cfg.MaxAttempts = 2
			cfg.RetryPolicy = bus.ConstantBackoff{Delay: 0}
			b, err := bus.Open(cfg, bus.WithTransport(tr))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := b.Close(cleanup); err != nil {
					t.Error("broker cleanup failed")
				}
			})
			store := f.Store
			var businessCalls atomic.Int32
			var reject atomic.Bool
			handler, err := consumer.WrapInbox[T](f.Inbox, consumer.Config{Namespace: cfg.Namespace, Consumer: "billing", Tenant: func(_ context.Context, j bus.Job) (string, error) {
				// Fixture trust policy only accepts these two explicit tenants.
				tenant := j.Header(publisher.HeaderTenant)
				if tenant != "a" && tenant != "b" {
					return "", messaging.ErrInvalidMessage
				}
				return tenant, nil
			}}, func(_ context.Context, tx T, j bus.Job) error {
				var payload struct {
					ID int64 `json:"id"`
				}
				if err := j.Decode(&payload); err != nil {
					return err
				}
				if payload.ID != 9223372036854775807 || j.Header("business") != "preserved" {
					return messaging.ErrInvalidMessage
				}
				businessCalls.Add(1)
				if err := f.Effect(tx, j.Header(publisher.HeaderTenant)+":"+j.Header(publisher.HeaderMessageID)); err != nil {
					return err
				}
				if j.Header(publisher.HeaderMessageID) == "dlq" && reject.Load() {
					return messaging.ErrInvalidMessage
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := b.DeclareTask("created"); err != nil {
				t.Fatal(err)
			}
			if err := b.HandleTask("created", handler); err != nil {
				t.Fatal(err)
			}
			eventOptions := []bus.EventOption{}
			if name == "nats" {
				eventOptions = append(eventOptions, natsdriver.WithEventLayoutTargeted())
			} else if name == "redis" {
				eventOptions = append(eventOptions, redisdriver.WithEventLayoutTargeted())
			} else if name == "kafka" {
				eventOptions = append(eventOptions, kafkadriver.WithEventLayoutTargeted())
			}
			if err := b.DeclareEvent("changed", eventOptions...); err != nil {
				t.Fatal(err)
			}
			if err := b.Subscribe("changed", "billing", handler); err != nil {
				t.Fatal(err)
			}
			if err := b.Start(ctx); err != nil {
				t.Fatal(err)
			}
			publisher, err := publisher.NewPublisher(b, publisher.Config{Namespace: cfg.Namespace})
			if err != nil {
				t.Fatal(err)
			}
			m := messaging.Message{ID: "same-business-id", Namespace: cfg.Namespace, TenantID: "a", Kind: messaging.Task, Type: "created", SchemaVersion: 1, Payload: []byte(`{"id":9223372036854775807}`), Headers: map[string]string{"business": "preserved"}}
			appendOne := func(m messaging.Message) {
				t.Helper()
				if err := f.Within(ctx, func(tx T) error { return f.Append(tx, m) }); err != nil {
					t.Fatal(err)
				}
			}
			// A process crashed after claiming but before publishing.
			appendOne(m)
			claims, err := store.Claim(ctx, outbox.ClaimRequest{Limit: 1, MaxAttempts: 20, Lease: time.Minute})
			if err != nil || len(claims) != 1 {
				t.Fatal(claims, err)
			}
			f.Expire(ctx, m.Identity())
			fault := &publicationFault{publisher: publisher}
			fault.drop.Store(true)
			dcfg := outbox.DefaultConfig()
			dcfg.PollInterval = 10 * time.Millisecond
			dcfg.RetryMin = 20 * time.Millisecond
			dcfg.RetryMax = 50 * time.Millisecond
			d, err := outbox.NewDispatcher(store, fault, dcfg)
			if err != nil {
				t.Fatal(err)
			}
			run := make(chan error, 1)
			go func() { run <- d.Run(ctx) }()
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = d.Drain(cleanup)
				<-run
			})
			await(t, 15*time.Second, func() bool {
				r, e := store.Get(ctx, "a", m.ID)
				return e == nil && r.State == outbox.Published && f.Count(ctx, "a:"+m.ID) == 1
			})
			// Explicitly redeliver to exercise commit-before-ACK dedupe on brokers
			// that do not automatically suppress the identical wire ID.
			if err := publisher.Publish(ctx, m); err != nil {
				t.Fatal(err)
			}
			m.TenantID = "b"
			appendOne(m)
			await(t, 15*time.Second, func() bool { return f.Count(ctx, "b:"+m.ID) == 1 })
			m.ID = "event"
			m.Kind = messaging.Event
			m.Type = "changed"
			appendOne(m)
			await(t, 15*time.Second, func() bool { return f.Count(ctx, "b:event") == 1 })
			if businessCalls.Load() != 3 {
				t.Fatal("duplicate business writes", businessCalls.Load())
			}
			// A failed transaction leaves no inbox record. Broker retry then DLQ
			// recovery must preserve payload precision and original identity.
			reject.Store(true)
			m.ID = "dlq"
			m.Kind = messaging.Task
			m.Type = "created"
			appendOne(m)
			ref := bus.TaskRef("created")
			await(t, 15*time.Second, func() bool { n, e := b.CountDeadLetters(ctx, ref); return e == nil && n == 1 })
			if f.Count(ctx, "b:dlq") != 0 {
				t.Fatal("failed consumer committed business state")
			}
			page, err := b.ListDeadLetters(ctx, ref, "", 10)
			if err != nil || len(page.Records) != 1 {
				t.Fatal(page, err)
			}
			reject.Store(false)
			if err := b.RequeueDeadLetters(ctx, ref, []string{page.Records[0].ID}); err != nil {
				t.Fatal(err)
			}
			await(t, 15*time.Second, func() bool { return f.Count(ctx, "b:dlq") == 1 })
		})
	}
}
func await(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not converge")
}

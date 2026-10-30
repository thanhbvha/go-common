//go:build integration

package eventbus_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	bus "github.com/thanhbvha/go-common/eventbus"
	kafkabus "github.com/thanhbvha/go-common/eventbus/driver/kafka"
	"github.com/thanhbvha/go-common/eventbus/transport"
	commonkafka "github.com/thanhbvha/go-common/kafka"
	bridge "github.com/thanhbvha/go-common/websocket/integration/eventbus"
	"github.com/thanhbvha/go-common/websocket/routing"
	natsrouting "github.com/thanhbvha/go-common/websocket/routing/nats"
	redisrouting "github.com/thanhbvha/go-common/websocket/routing/redis"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Lose one result only after an actual successful enqueue, not after publication.
type loseEnqueueResult struct {
	routing.DeliveryTransport
	lose     atomic.Bool
	attempts atomic.Int64
}

func (t *loseEnqueueResult) Deliver(ctx context.Context, node string, c routing.DeliveryCommand) (routing.DeliveryResult, error) {
	t.attempts.Add(1)
	result, err := t.DeliveryTransport.Deliver(ctx, node, c)
	if err == nil && result.Count(routing.Enqueued) > 0 && t.lose.CompareAndSwap(true, false) {
		return routing.DeliveryResult{}, context.DeadlineExceeded
	}
	return result, err
}
func TestKafkaMixedDelivery(t *testing.T) {
	if os.Getenv("WS_KAFKA_INTEGRATION") != "1" {
		t.Skip("set WS_KAFKA_INTEGRATION=1 and WS_KAFKA_BROKERS for Kafka integration")
	}
	brokers := strings.Split(os.Getenv("WS_KAFKA_BROKERS"), ",")
	if len(brokers) == 0 || brokers[0] == "" {
		t.Fatal("WS_KAFKA_BROKERS is required when Kafka integration is enabled")
	}
	for _, backend := range []string{"redis", "nats"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			namespace := "ws_mixed_" + routing.NewID()
			cfg := commonkafka.DefaultConfig()
			cfg.Brokers = brokers
			adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
			if err != nil {
				t.Fatal(err)
			}
			defer adminClient.Close()
			// Only resources generated from this test's unique namespace may be deleted.
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				admin := kadm.NewClient(adminClient)
				details, err := admin.ListTopics(cleanup)
				if err != nil {
					t.Error(err)
					return
				}
				var topics []string
				roots := []string{(transport.Topology{Namespace: namespace, Kind: transport.TaskTopology, JobType: "ws.command"}).Name(), (transport.Topology{Namespace: namespace, Kind: transport.EventTopology, JobType: "ws.notification"}).Name()}
				for topic := range details {
					for _, root := range roots {
						if topic == root || strings.HasPrefix(topic, root+".") {
							topics = append(topics, topic)
							break
						}
					}
				}
				if len(topics) > 0 {
					if _, err = admin.DeleteTopics(cleanup, topics...); err != nil {
						t.Error(err)
					}
				}
			}()
			openBus := func() bus.Bus {
				tr, err := kafkabus.NewWithConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				c := bus.DefaultConfig()
				c.Namespace = namespace
				c.RetryPolicy = bus.ConstantBackoff{Delay: time.Millisecond}
				b, err := bus.Open(c, bus.WithTransport(tr))
				if err != nil {
					tr.Close(context.Background())
					t.Fatal(err)
				}
				t.Cleanup(func() {
					shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
					defer stop()
					b.Close(shutdown)
				})
				return b
			}
			client := realRedis(t)
			registry := redisrouting.NewRegistry(client, namespace)
			newTransport := func() routing.DeliveryTransport { return redisrouting.NewTransport(client, namespace) }
			if backend == "nats" {
				// No JetStream here: Kafka owns business persistence, NATS only delivery.
				s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: server.RANDOM_PORT, NoLog: true, NoSigs: true})
				if err != nil {
					t.Fatal(err)
				}
				go s.Start()
				if !s.ReadyForConnections(5 * time.Second) {
					t.Fatal("NATS startup timeout")
				}
				defer func() { s.Shutdown(); s.WaitForShutdown() }()
				nc, err := gonats.Connect(s.ClientURL())
				if err != nil {
					t.Fatal(err)
				}
				defer nc.Close()
				newTransport = func() routing.DeliveryTransport { return natsrouting.NewTransport(nc, namespace) }
			}
			gateway := func() *routing.Gateway {
				g, err := routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: newTransport()})
				if err != nil {
					t.Fatal(err)
				}
				if err = g.Start(ctx); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					shutdown, stop := context.WithTimeout(context.Background(), time.Second)
					defer stop()
					g.Close(shutdown)
				})
				return g
			}
			a, b := gateway(), gateway()
			framesA, framesB := make(chan string, 4), make(chan string, 4)
			var full atomic.Bool
			full.Store(true)
			var accepted atomic.Int64
			old, err := a.Register(ctx, routing.Location{TenantID: "t", UserID: "u", SessionID: "device"}, func(payload []byte) error {
				if full.CompareAndSwap(true, false) {
					return routing.ErrQueueFull
				}
				accepted.Add(1)
				framesA <- string(payload)
				return nil
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			sender := newTransport()
			if err = sender.Start(ctx, routing.NewID(), nil); err != nil {
				t.Fatal(err)
			}
			defer sender.Close(context.Background())
			lossy := &loseEnqueueResult{DeliveryTransport: sender}
			lossy.lose.Store(true)
			router, err := routing.NewRouter(routing.RouterConfig{Registry: registry, Transport: lossy, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			consumer, producer := openBus(), openBus()
			if _, err = bridge.New(bridge.Config{Bus: producer}); !errors.Is(err, bus.ErrDedupeUnavailable) {
				t.Fatalf("Kafka must reject required dedupe at construction: %v", err)
			}
			options := []bus.EventOption{kafkabus.WithEventLayoutTargeted(), kafkabus.WithFetchMaxWait(25 * time.Millisecond)}
			_, err = bridge.New(bridge.Config{Bus: consumer, Router: router, PublicationDedupe: bridge.DedupeDisabled, NotificationOptions: options})
			if err != nil {
				t.Fatal(err)
			}
			publisher, err := bridge.New(bridge.Config{Bus: producer, PublicationDedupe: bridge.DedupeDisabled, NotificationOptions: options})
			if err != nil {
				t.Fatal(err)
			}
			if err = consumer.Start(ctx); err != nil {
				t.Fatal(err)
			}
			defer consumer.Close(context.Background())
			if err = producer.Start(ctx); err != nil {
				t.Fatal(err)
			}
			defer producer.Close(context.Background())
			publish := func(id, user, room, payload string) {
				t.Helper()
				if err := publisher.PublishNotification(ctx, bridge.Notification{MessageID: id, TenantID: "t", UserID: user, RoomID: room, Payload: []byte(payload)}); err != nil {
					t.Fatal(err)
				}
			}
			receive := func(frames <-chan string, want string) {
				t.Helper()
				select {
				case got := <-frames:
					if got != want {
						t.Fatalf("frame=%q want=%q", got, want)
					}
				case <-ctx.Done():
					t.Fatalf("missing %q: %v", want, ctx.Err())
				}
			}
			publish("retry-result", "u", "", "retry")
			receive(framesA, "retry")
			for {
				stats, err := consumer.SubscriberStats(ctx, "ws.notification", "realtime-router")
				if err != nil {
					t.Fatal(err)
				}
				if lossy.attempts.Load() >= 3 && stats.Pending == 0 && stats.InFlight == 0 && stats.Delayed == 0 {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatalf("retry settlement timeout: %+v", stats)
				case <-time.After(20 * time.Millisecond):
				}
			}
			if accepted.Load() != 1 {
				t.Fatalf("lost result caused duplicate enqueue: %d", accepted.Load())
			}
			// Reconnect moves the same logical session; old cleanup must not erase it.
			location, err := b.Register(ctx, routing.Location{TenantID: "t", UserID: "u", SessionID: "device"}, func(p []byte) error { framesB <- string(p); return nil }, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = a.Remove(ctx, old); err != nil {
				t.Fatal(err)
			}
			resolved, err := registry.Resolve(ctx, "t", "u", "device")
			if err != nil || len(resolved) != 1 || resolved[0].Generation != location.Generation {
				t.Fatalf("reconnect fencing failed: %+v %v", resolved, err)
			}
			publish("reconnected", "u", "", "moved")
			receive(framesB, "moved")
			other, err := a.Register(ctx, routing.Location{TenantID: "t", UserID: "other", SessionID: "other"}, func(p []byte) error { framesA <- string(p); return nil }, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = a.JoinRoom(ctx, other, "room"); err != nil {
				t.Fatal(err)
			}
			if err = b.JoinRoom(ctx, location, "room"); err != nil {
				t.Fatal(err)
			}
			publish("room", "", "room", "room")
			receive(framesA, "room")
			receive(framesB, "room")
		})
	}
}

package eventbus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/driver/memory"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

type benchmarkPayload struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
}

func BenchmarkGuardNoPanic(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if guardCall(panicScopeMetrics, "benchmark", nil, func() {}) {
			b.Fatal("unexpected panic")
		}
	}
}

func BenchmarkCodecEncode(b *testing.B) {
	codec := NewCodec(nil)
	payload := benchmarkPayload{ID: 9_007_199_254_740_993, Name: "benchmark", Enabled: true}

	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := codec.EncodeTopology(TaskTopology, "benchmark.codec", payload, 5); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodecDecode(b *testing.B) {
	codec := NewCodec(nil)
	payload := benchmarkPayload{ID: 9_007_199_254_740_993, Name: "benchmark", Enabled: true}
	data, _, err := codec.EncodeTopology(TaskTopology, "benchmark.codec", payload, 5)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := codec.Decode(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnqueue(b *testing.B) {
	sink := &benchmarkTransport{}
	cfg := DefaultConfig()
	bus, err := Open(cfg, WithTransport(sink))
	if err != nil {
		b.Fatal(err)
	}
	if err := bus.DeclareTask("benchmark.enqueue"); err != nil {
		b.Fatal(err)
	}
	if err := bus.HandleTask("benchmark.enqueue", func(context.Context, Job) error { return nil }); err != nil {
		b.Fatal(err)
	}
	payload := benchmarkPayload{ID: 42, Name: "benchmark", Enabled: true}

	b.ReportAllocs()
	for b.Loop() {
		if err := bus.Enqueue(context.Background(), "benchmark.enqueue", payload); err != nil {
			b.Fatal(err)
		}
	}
	if got := sink.published.Load(); got != int64(b.N) {
		b.Fatalf("published = %d, want %d", got, b.N)
	}
}

func BenchmarkEngineDispatch(b *testing.B) {
	t := memory.New()
	cfg := DefaultConfig()
	cfg.Concurrency = 1
	bus, err := Open(cfg, WithTransport(t))
	if err != nil {
		b.Fatal(err)
	}

	completed := make(chan struct{})
	if err := bus.DeclareTask("benchmark.dispatch"); err != nil {
		b.Fatal(err)
	}
	if err := bus.HandleTask("benchmark.dispatch", func(context.Context, Job) error {
		completed <- struct{}{}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := bus.Start(ctx); err != nil {
		b.Fatal(err)
	}

	payload := benchmarkPayload{ID: 42, Name: "benchmark", Enabled: true}
	b.ReportAllocs()
	for b.Loop() {
		if err := bus.Enqueue(ctx, "benchmark.dispatch", payload); err != nil {
			b.Fatal(err)
		}
		<-completed
	}
	if err := bus.Stop(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkMemoryTransportRoundTrip(b *testing.B) {
	t := memory.New()
	topo := transport.Topology{Namespace: "benchmark", JobType: "roundtrip"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := t.EnsureTopology(ctx, topo); err != nil {
		b.Fatal(err)
	}

	deliveries := make(chan transport.Delivery)
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- t.Consume(ctx, transport.Subscription{Topology: topo}, func(d transport.Delivery) {
			deliveries <- d
		})
	}()

	msg := transport.Message{
		ID:      "benchmark-message",
		Type:    topo.JobType,
		Payload: []byte(`{"v":2,"id":"benchmark-message","type":"roundtrip","payload":{}}`),
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(msg.Payload)))
	for b.Loop() {
		if err := t.Publish(ctx, topo, msg); err != nil {
			b.Fatal(err)
		}
		delivery := <-deliveries
		if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			b.Fatal(err)
		}
	}
	cancel()
	select {
	case <-consumeDone:
	case <-time.After(time.Second):
		b.Fatal("memory transport consumer did not stop")
	}
	if err := t.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}

type benchmarkTransport struct {
	published atomic.Int64
}

func (*benchmarkTransport) Name() string { return "benchmark" }

func (*benchmarkTransport) Capabilities() transport.Capabilities {
	return transport.Capabilities{DelayPrecision: time.Millisecond, Ordering: true}
}

func (*benchmarkTransport) EnsureTopology(context.Context, transport.Topology) error { return nil }

func (t *benchmarkTransport) Publish(context.Context, transport.Topology, transport.Message) error {
	t.published.Add(1)
	return nil
}

func (t *benchmarkTransport) PublishAt(context.Context, transport.Topology, transport.Message, time.Time) error {
	t.published.Add(1)
	return nil
}

func (*benchmarkTransport) Consume(context.Context, transport.Subscription, func(transport.Delivery)) error {
	return nil
}

func (*benchmarkTransport) Ping(context.Context) error { return nil }

func (*benchmarkTransport) Close(context.Context) error { return nil }

package eventbus

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type runtimeTestMetrics struct {
	noopMetrics
	mu                                                sync.Mutex
	enqueue                                           []string
	dequeue, panics, timeouts, reconnects, saturation int
	settlements                                       []string
	complete                                          []string
	ages                                              []float64
	stats                                             []transport.TopologyStats
}

func (m *runtimeTestMetrics) IncEnqueue(_ string, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enqueue = append(m.enqueue, status)
}
func (m *runtimeTestMetrics) IncDequeue(string) { m.mu.Lock(); defer m.mu.Unlock(); m.dequeue++ }
func (m *runtimeTestMetrics) IncComplete(_ string, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.complete = append(m.complete, status)
}
func (m *runtimeTestMetrics) IncHandlerFailure(_ string, outcome string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if outcome == "panic" {
		m.panics++
	}
	if outcome == "timeout" {
		m.timeouts++
	}
}
func (m *runtimeTestMetrics) ObserveSettlement(_ string, decision, outcome string, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.settlements = append(m.settlements, decision+":"+outcome)
}
func (m *runtimeTestMetrics) IncReconnect(string) { m.mu.Lock(); defer m.mu.Unlock(); m.reconnects++ }
func (m *runtimeTestMetrics) ObserveSaturation(_ string, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saturation++
}
func (m *runtimeTestMetrics) ObserveQueueAge(_ string, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ages = append(m.ages, seconds)
}
func (m *runtimeTestMetrics) ObserveStats(_ string, stats transport.TopologyStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats = append(m.stats, stats)
}

type traceKey struct{}

type observationTransport struct {
	benchmarkTransport
	message    transport.Message
	failure    error
	reserved   bool
	reserveTTL time.Duration
	stats      transport.TopologyStats
}

func (b *observationTransport) Publish(_ context.Context, _ transport.Topology, msg transport.Message) error {
	b.message = msg
	return b.failure
}
func (b *observationTransport) PublishAt(ctx context.Context, topo transport.Topology, msg transport.Message, _ time.Time) error {
	return b.Publish(ctx, topo, msg)
}
func (b *observationTransport) Reserve(_ context.Context, _ string, ttl time.Duration) (bool, error) {
	b.reserveTTL = ttl
	return b.reserved, nil
}
func (*observationTransport) Release(context.Context, string) error { return nil }
func (b *observationTransport) Stats(context.Context, transport.Topology) (transport.TopologyStats, error) {
	return b.stats, b.failure
}

type traceTestRecorder struct {
	mu           sync.Mutex
	starts, ends int
	carriers     []Headers
	events       []string
}

func (r *traceTestRecorder) Start(ctx context.Context, name string, carrier Headers) (context.Context, func()) {
	r.mu.Lock()
	r.starts++
	r.carriers = append(r.carriers, cloneHeaders(carrier))
	r.mu.Unlock()
	return context.WithValue(ctx, traceKey{}, name), func() { r.mu.Lock(); defer r.mu.Unlock(); r.ends++ }
}
func (*traceTestRecorder) Inject(_ context.Context, carrier Headers) {
	carrier["traceparent"] = "synthetic-trace"
}
func (r *traceTestRecorder) RecordEvent(_ context.Context, name string, attrs map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, name+":"+attrs["outcome"])
}

// DS-41/44: all settlement kinds report failures and latency without claiming
// completion. Success is recorded only after the subsequent confirmation.
func TestRuntimeSettlementMetrics(t *testing.T) {
	for _, kind := range []transport.DecisionKind{transport.DecisionAck, transport.DecisionRetry, transport.DecisionDeadLetter} {
		t.Run(kind.String(), func(t *testing.T) {
			m := &runtimeTestMetrics{}
			r := &traceTestRecorder{}
			cfg := DefaultConfig()
			cfg.Metrics = m
			cfg.Tracer = r
			e := newEngine(cfg, nil, NewCodec(nil))
			defer e.cancel()
			defer e.workerCancel()
			d := &settlementTestDelivery{failures: 1}
			e.settleDelivery(d, transport.Decision{Kind: kind}, "task", "synthetic-id")
			if len(m.complete) != 0 || len(m.settlements) != 1 || m.settlements[0] != kind.String()+":error" {
				t.Fatal("failed settlement counted as complete or not observed")
			}
			e.settleDelivery(d, transport.Decision{Kind: kind}, "task", "synthetic-id")
			if len(m.complete) != 1 || m.complete[0] != kind.String() || m.settlements[1] != kind.String()+":success" {
				t.Fatal("confirmation was not observed")
			}
			if r.starts != r.ends {
				t.Fatal("settlement span leaked")
			}
		})
	}
}

func TestTracingAndHeadersSurvivePublishRetryAndDLQ(t *testing.T) {
	m := &runtimeTestMetrics{}
	r := &traceTestRecorder{}
	cfg := DefaultConfig()
	cfg.Metrics = m
	cfg.Tracer = r
	cfg.RetryPolicy = ConstantBackoff{}
	backend := &observationTransport{}
	b, err := Open(cfg, WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.DeclareTask("traced"); err != nil {
		t.Fatal(err)
	}
	headers := Headers{"correlation-id": "synthetic-correlation"}
	if err := b.Enqueue(context.Background(), "traced", 1, func(o *JobOptions) { o.Headers = headers }); err != nil {
		t.Fatal(err)
	}
	if headers["traceparent"] != "" {
		t.Fatal("tracer mutated caller headers")
	}
	eng := b.(*bus).engine
	published, err := eng.codec.Decode(backend.message.Payload)
	if err != nil || published.Headers["traceparent"] != "synthetic-trace" || backend.message.Headers["correlation-id"] != "synthetic-correlation" {
		t.Fatal("producer did not inject durable propagation headers")
	}
	payload, _, err := eng.codec.EncodeTopology(TaskTopology, "traced", 1, 2, WithHeader("traceparent", "synthetic-trace"), WithHeader("correlation-id", "synthetic-correlation"))
	if err != nil {
		t.Fatal(err)
	}
	d := &attemptDelivery{message: transport.Message{Payload: payload}, deliveryCount: 1}
	eng.processDelivery(TaskTopology, "traced", "", func(ctx context.Context, j Job) error {
		if ctx.Value(traceKey{}) != "eventbus.process" || j.Header("correlation-id") != "synthetic-correlation" {
			t.Fatal("handler missing trace context or correlation")
		}
		return errors.New("temporary")
	}, d, 0)
	if len(d.retries) != 1 || d.retries[0].Headers["traceparent"] != "synthetic-trace" {
		t.Fatal("retry propagation lost")
	}
	next := &attemptDelivery{message: d.retries[0], deliveryCount: 1}
	eng.processDelivery(TaskTopology, "traced", "", func(context.Context, Job) error { return MarkPermanent(errors.New("synthetic failure")) }, next, 0)
	if len(next.deadLetters) != 1 {
		t.Fatal("missing DLQ")
	}
	env, err := eng.codec.Decode(next.deadLetters[0].Message.Payload)
	if err != nil || env.Headers["traceparent"] != "synthetic-trace" || env.Headers["correlation-id"] != "synthetic-correlation" {
		t.Fatal("DLQ lost durable propagation headers")
	}
	if m.dequeue != 2 || r.starts != r.ends {
		t.Fatal("missing dequeue or unclosed trace spans")
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEnqueueOutcomesAndStatsHooks(t *testing.T) {
	m := &runtimeTestMetrics{}
	cfg := DefaultConfig()
	cfg.Metrics = m
	backend := &observationTransport{reserved: true, stats: transport.TopologyStats{Lag: 9, LagKnown: true, OldestAge: 3 * time.Second, OldestAgeKnown: true}}
	b, err := Open(cfg, WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(context.Background())
	if err := b.DeclareTask("task"); err != nil {
		t.Fatal(err)
	}
	if err := b.Enqueue(context.Background(), "task", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Stats(context.Background(), "task"); err != nil {
		t.Fatal(err)
	}
	if len(m.stats) != 1 || m.stats[0] != backend.stats {
		t.Fatal("lag/oldest age sample or availability flags lost")
	}
	backend.failure = errors.New("unavailable")
	if err := b.Enqueue(context.Background(), "task", 1, WithDedupeKey("synthetic")); err == nil {
		t.Fatal("publish failure hidden")
	}
	if _, err := b.Stats(context.Background(), "task"); err == nil {
		t.Fatal("stats failure hidden")
	}
	if len(m.stats) != 1 {
		t.Fatal("failed stats emitted fabricated data")
	}
	backend.failure = nil
	backend.reserved = false
	if err := b.Enqueue(context.Background(), "task", 1, WithDedupeKey("synthetic")); err != nil {
		t.Fatal(err)
	}
	if len(m.enqueue) != 3 || m.enqueue[0] != "success" || m.enqueue[1] != "error" || m.enqueue[2] != "duplicate" {
		t.Fatalf("enqueue outcomes = %v", m.enqueue)
	}
}

func TestEngineHonorsRequestedDedupeTTL(t *testing.T) {
	backend := &observationTransport{reserved: true}
	b, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(context.Background())
	if err := b.DeclareTask("task"); err != nil {
		t.Fatal(err)
	}
	if err := b.Enqueue(context.Background(), "task", 1, WithDedupeKey("key"), WithDedupeTTL(375*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if backend.reserveTTL != 375*time.Millisecond {
		t.Fatalf("reservation TTL = %v, want 375ms", backend.reserveTTL)
	}
	if err := b.Enqueue(context.Background(), "task", 1, WithDedupeKey("default")); err != nil {
		t.Fatal(err)
	}
	if backend.reserveTTL != 24*time.Hour {
		t.Fatalf("default reservation TTL = %v, want 24h", backend.reserveTTL)
	}
}

func TestHandlerTimeoutAndPanicHooks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := &runtimeTestMetrics{}
		r := &traceTestRecorder{}
		cfg := DefaultConfig()
		cfg.Metrics = m
		cfg.Tracer = r
		cfg.ProcessTimeout = time.Second
		e := newEngine(cfg, nil, NewCodec(nil))
		defer e.cancel()
		defer e.workerCancel()
		payload, _, err := e.codec.EncodeTopology(TaskTopology, "task", 1, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range []Handler{
			func(ctx context.Context, _ Job) error { <-ctx.Done(); return nil },
			func(context.Context, Job) error { panic("synthetic") },
		} {
			d := &attemptDelivery{message: transport.Message{Payload: payload}, deliveryCount: 1}
			e.processDelivery(TaskTopology, "task", "", h, d, 0)
			if len(d.retries) != 1 || d.acks != 0 {
				t.Fatal("timeout/panic incorrectly acknowledged")
			}
		}
		if m.timeouts != 1 || m.panics != 1 || len(m.ages) != 2 || r.starts != r.ends {
			t.Fatal("missing runtime failure/age hooks or leaked spans")
		}
	})
}

func TestReconnectAndSaturationHooks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := &runtimeTestMetrics{}
		cfg := DefaultConfig()
		cfg.Metrics = m
		cfg.Concurrency = 1
		cfg.Prefetch = 1
		payload, _, err := NewCodec(nil).EncodeTopology(TaskTopology, "task", 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		release := make(chan struct{})
		calls := 0
		backend := &supervisorTransport{consume: func(ctx context.Context, _ transport.Subscription, deliver func(transport.Delivery)) error {
			calls++
			if calls == 1 {
				return errors.New("disconnected")
			}
			for range 4 {
				deliver(&workerPoolDelivery{message: transport.Message{Payload: payload}, settled: make(chan struct{})})
			}
			<-ctx.Done()
			return ctx.Err()
		}}
		b, err := Open(cfg, WithTransport(backend))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Register("task", func(context.Context, Job) error { <-release; return nil }); err != nil {
			t.Fatal(err)
		}
		if err := b.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(150 * time.Millisecond)
		synctest.Wait()
		if m.reconnects != 1 {
			t.Fatalf("reconnects = %d", m.reconnects)
		}
		close(release)
		synctest.Wait()
		if err := b.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if m.saturation == 0 {
			t.Fatal("backpressure not observed")
		}
	})
}

package demo

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus"
	kafkadriver "github.com/thanhbvha/go-common/eventbus/driver/kafka"
	natsdriver "github.com/thanhbvha/go-common/eventbus/driver/nats"
	redisdriver "github.com/thanhbvha/go-common/eventbus/driver/redis"
	"github.com/thanhbvha/go-common/eventbus/transport"
	commonkafka "github.com/thanhbvha/go-common/kafka"
)

func TestMemoryRuntimeEndToEnd(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Driver = "memory"
	cfg.Namespace = "example_test"
	logger := NewLogger("test")
	t.Cleanup(logger.Close)
	rt, err := OpenRuntime(context.Background(), cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	bus, err := NewBus(cfg, rt, logger, NewMetrics(), NewTracer(logger))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := CloseAll(bus, rt); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	if err := bus.DeclareTask(TaskName, TopologyOptions("memory")...); err != nil {
		t.Fatal(err)
	}
	received := make(chan UserMessage, 1)
	if err := bus.HandleTask(TaskName, eventbus.Handle(func(_ context.Context, _ eventbus.Job, payload UserMessage) error {
		received <- payload
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := Start(context.Background(), bus); err != nil {
		t.Fatal(err)
	}
	want := UserMessage{UserID: "test-user", Email: "test@example.invalid"}
	if err := bus.Enqueue(context.Background(), TaskName, want); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if got != want {
			t.Fatalf("payload = %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for memory delivery")
	}
}

func TestConfigValidationAndCapabilities(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Driver = "unknown"
	if err := cfg.Validate(); err == nil {
		t.Fatal("unsupported driver accepted")
	}
	cfg.Driver = "memory"
	logger := NewLogger("test")
	t.Cleanup(logger.Close)
	rt, err := OpenRuntime(context.Background(), cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if !SupportsDedupe(rt.Transport) {
		t.Fatal("memory transport should advertise topology dedupe")
	}
}

func TestRuntimeCloseReportsDriverError(t *testing.T) {
	want := errors.New("close failed")
	rt := &Runtime{driver: "redis", close: func() error { return want }}
	err := rt.Close()
	if !errors.Is(err, want) {
		t.Fatalf("Close error = %v, want wrapped close error", err)
	}
}

func TestLoadConfigFromYAMLAndEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "eventbus.yaml")
	if err := os.WriteFile(path, []byte("driver: memory\nnamespace: from_file\nwait: 3s\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EVENTBUS_NAMESPACE", "from_environment")
	cfg, err := LoadConfig([]string{"--config", path})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Namespace != "from_environment" {
		t.Fatalf("Namespace = %q, want environment override", cfg.Namespace)
	}
	if cfg.Wait != 3*time.Second {
		t.Fatalf("Wait = %s, want 3s", cfg.Wait)
	}
}

func TestLoadConfigFromDotEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("DRIVER=memory\nNAMESPACE=from_dotenv\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig([]string{"--config=" + path})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Namespace != "from_dotenv" {
		t.Fatalf("Namespace = %q, want from_dotenv", cfg.Namespace)
	}
}

func TestKafkaEventExampleTopologyIsValid(t *testing.T) {
	backend, err := kafkadriver.NewWithConfig(commonkafka.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close(context.Background())
	bus, err := eventbus.Open(eventbus.DefaultConfig(), eventbus.WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := DeclareEvent(bus, "kafka"); err != nil {
		t.Fatalf("Kafka example DeclareEvent: %v", err)
	}
	validator := backend.(transport.TopologyValidator)
	if err := validator.ValidateTopology(transport.Topology{JobType: EventName, Kind: transport.EventTopology}); err == nil {
		t.Fatal("Kafka event topology without the example option was accepted")
	}
}

func TestNATSEventExampleTopologyIncludesMigrationFence(t *testing.T) {
	topology := transport.Topology{JobType: EventName, Kind: transport.EventTopology}
	for _, option := range EventTopologyOptions("nats") {
		option(&topology)
	}
	validator := natsdriver.New(nil).(transport.TopologyValidator)
	if err := validator.ValidateTopology(topology); err != nil {
		t.Fatalf("NATS example event topology: %v", err)
	}
	if err := validator.ValidateTopology(transport.Topology{JobType: EventName, Kind: transport.EventTopology}); err == nil {
		t.Fatal("NATS event topology without the example option was accepted")
	}
}

func TestRedisEventExampleTopologyIncludesMigrationFence(t *testing.T) {
	topology := transport.Topology{JobType: EventName, Kind: transport.EventTopology}
	for _, option := range EventTopologyOptions("redis") {
		option(&topology)
	}
	backend := redisdriver.New(nil)
	defer backend.Close(context.Background())
	validator := backend.(transport.TopologyValidator)
	if err := validator.ValidateTopology(topology); err != nil {
		t.Fatalf("Redis example event topology: %v", err)
	}
	if err := validator.ValidateTopology(transport.Topology{JobType: EventName, Kind: transport.EventTopology}); err == nil {
		t.Fatal("Redis event topology without the example option was accepted")
	}
}

func TestSplitCSV(t *testing.T) {
	got := SplitCSV(" one, ,two ")
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("SplitCSV = %#v", got)
	}
}

func TestSafeNATSURLsRejectsCredentials(t *testing.T) {
	if _, err := safeNATSURLs("nats://user:secret@localhost:4222"); err == nil {
		t.Fatal("credential-bearing NATS URL accepted")
	}
	got, err := safeNATSURLs("nats://localhost:4222,tls://nats.example:4223")
	if err != nil || len(got) != 2 {
		t.Fatalf("safeNATSURLs = %#v, %v", got, err)
	}
}

func TestOTelAdaptersConstructWithStableMetricNames(t *testing.T) {
	if err := ValidateOTelMetricNames(); err != nil {
		t.Fatal(err)
	}
	metrics, err := NewOTelMetrics()
	if err != nil {
		t.Fatalf("NewOTelMetrics: %v", err)
	}
	metrics.IncEnqueue(TaskName, "success")
	tracer := NewOTelTracer()
	ctx, end := tracer.Start(context.Background(), "eventbus.publish", nil)
	tracer.RecordEvent(ctx, "publish", map[string]string{"outcome": "success"})
	end()
}

func TestMemoryOperationalControlsEndToEnd(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Driver = "memory"
	cfg.Namespace = "example_controls_test"
	logger := NewLogger("test")
	t.Cleanup(logger.Close)
	rt, err := OpenRuntime(context.Background(), cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	bus, err := NewBus(cfg, rt, logger, NewMetrics(), NewTracer(logger))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := CloseAll(bus, rt); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	if err := bus.DeclareTask(TaskName, eventbus.WithConcurrency(1), eventbus.WithPrefetch(1)); err != nil {
		t.Fatal(err)
	}
	received := make(chan int, 2)
	if err := bus.HandleTask(TaskName, eventbus.Handle(func(_ context.Context, _ eventbus.Job, value int) error {
		received <- value
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := Start(context.Background(), bus); err != nil {
		t.Fatal(err)
	}
	if err := bus.Pause(context.Background(), TaskName); err != nil {
		t.Fatal(err)
	}
	if err := bus.EnqueueBatch(context.Background(), TaskName, []eventbus.BatchJob{{Payload: 1}, {Payload: 2}}); err != nil {
		t.Fatal(err)
	}
	if err := bus.DrainTopology(context.Background(), TaskName); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-received:
		t.Fatalf("received %d while paused", value)
	case <-time.After(25 * time.Millisecond):
	}
	if err := bus.Resume(context.Background(), TaskName); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-received:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for resumed delivery")
		}
	}
}

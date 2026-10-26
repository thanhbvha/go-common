package eventbus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "blank namespace", mutate: func(c *Config) { c.Namespace = "   " }},
		{name: "namespace surrounding whitespace", mutate: func(c *Config) { c.Namespace = " app" }},
		{name: "blank driver", mutate: func(c *Config) { c.Driver = " " }},
		{name: "negative process timeout", mutate: func(c *Config) { c.ProcessTimeout = -time.Second }},
		{name: "negative concurrency", mutate: func(c *Config) { c.Concurrency = -1 }},
		{name: "negative prefetch", mutate: func(c *Config) { c.Prefetch = -1 }},
		{name: "negative max attempts", mutate: func(c *Config) { c.MaxAttempts = -1 }},
		{name: "negative max payload bytes", mutate: func(c *Config) { c.MaxPayloadBytes = -1 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.mutate(&cfg)
			if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestPublicAPIsRejectInvalidNames(t *testing.T) {
	tests := []struct {
		name string
		call func(Bus) error
	}{
		{name: "declare empty task", call: func(b Bus) error { return b.DeclareTask("") }},
		{name: "declare blank event", call: func(b Bus) error { return b.DeclareEvent("  ") }},
		{name: "handle padded task", call: func(b Bus) error { return b.HandleTask(" task", func(context.Context, Job) error { return nil }) }},
		{name: "subscribe empty event", call: func(b Bus) error {
			return b.Subscribe("", "subscriber", func(context.Context, Job) error { return nil })
		}},
		{name: "subscribe empty subscriber", call: func(b Bus) error { return b.Subscribe("event", "", func(context.Context, Job) error { return nil }) }},
		{name: "enqueue empty task", call: func(b Bus) error { return b.Enqueue(context.Background(), "", nil) }},
		{name: "publish blank event", call: func(b Bus) error { return b.Publish(context.Background(), " ", nil) }},
		{name: "stats empty task", call: func(b Bus) error { _, err := b.TopologyStats(context.Background(), TaskRef("")); return err }},
		{name: "stats empty event", call: func(b Bus) error { _, err := b.TopologyStats(context.Background(), EventRef("")); return err }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance, err := Open(DefaultConfig(), WithTransport(&benchmarkTransport{}))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := tt.call(instance); !errors.Is(err, ErrInvalidName) {
				t.Fatalf("error = %v, want ErrInvalidName", err)
			}
		})
	}
}

func TestHandlerRegistrationRejectsNilHandlersWithoutMutation(t *testing.T) {
	instance, err := Open(DefaultConfig(), WithTransport(&benchmarkTransport{}))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	concrete := instance.(*bus)
	if err := instance.DeclareTask("declared"); err != nil {
		t.Fatalf("DeclareTask: %v", err)
	}
	if err := instance.HandleTask("declared", nil); !errors.Is(err, ErrNilHandler) {
		t.Fatalf("HandleTask error = %v, want ErrNilHandler", err)
	}
	if err := instance.Subscribe("event", "subscriber", nil); !errors.Is(err, ErrNilHandler) {
		t.Fatalf("Subscribe error = %v, want ErrNilHandler", err)
	}
	if _, exists := concrete.engine.eventTopologies["event"]; exists {
		t.Fatal("nil Subscribe partially declared the event")
	}
}

func TestHandlePropagatesNilTypedHandlersToRegistrationValidation(t *testing.T) {
	instance, err := Open(DefaultConfig(), WithTransport(&benchmarkTransport{}))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var typed TypedHandler[string]
	if err := instance.DeclareTask("task"); err != nil {
		t.Fatalf("DeclareTask: %v", err)
	}
	if err := instance.HandleTask("task", Handle(typed)); !errors.Is(err, ErrNilHandler) {
		t.Fatalf("HandleTask error = %v, want ErrNilHandler", err)
	}
}

type rejectingTopologyTransport struct {
	benchmarkTransport
}

func (*rejectingTopologyTransport) ValidateTopology(transport.Topology) error {
	return fmt.Errorf("driver rejected topology")
}

func TestDeclarationInvokesDriverTopologyValidator(t *testing.T) {
	instance, err := Open(DefaultConfig(), WithTransport(&rejectingTopologyTransport{}))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := instance.DeclareTask("task"); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("DeclareTask error = %v, want ErrInvalidOption", err)
	}
}

func TestTopologyOptionsAreStrictlyValidated(t *testing.T) {
	tests := []struct {
		name string
		opts []TopologyOption
	}{
		{name: "zero concurrency", opts: []TopologyOption{WithConcurrency(0)}},
		{name: "negative concurrency", opts: []TopologyOption{WithConcurrency(-1)}},
		{name: "zero prefetch", opts: []TopologyOption{WithPrefetch(0)}},
		{name: "negative prefetch", opts: []TopologyOption{WithPrefetch(-1)}},
		{name: "zero max attempts", opts: []TopologyOption{WithMaxAttempts(0)}},
		{name: "negative max attempts", opts: []TopologyOption{WithMaxAttempts(-1)}},
		{name: "zero rate", opts: []TopologyOption{WithRateLimit(0, 1)}},
		{name: "negative rate", opts: []TopologyOption{WithRateLimit(-1, 1)}},
		{name: "zero rate burst", opts: []TopologyOption{WithRateLimit(1, 0)}},
		{name: "wrong rate type", opts: []TopologyOption{WithOption("eventbus.rate_limit", "fast")}},
		{name: "zero ordering key concurrency", opts: []TopologyOption{WithOrderingKeyConcurrency(0)}},
		{name: "negative ordering key concurrency", opts: []TopologyOption{WithOrderingKeyConcurrency(-1)}},
		{name: "zero max payload bytes", opts: []TopologyOption{WithMaxPayloadBytes(0)}},
		{name: "negative max payload bytes", opts: []TopologyOption{WithMaxPayloadBytes(-1)}},
		{name: "nil option", opts: []TopologyOption{nil}},
		{name: "empty option key", opts: []TopologyOption{WithOption("", 1)}},
		{name: "unknown core option", opts: []TopologyOption{WithOption("eventbus.unknown", 1)}},
		{name: "wrong core option type", opts: []TopologyOption{WithOption("eventbus.concurrency", "ten")}},
		{name: "option for another driver", opts: []TopologyOption{WithOption("redis.maxlen", int64(100))}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance, err := Open(DefaultConfig(), WithTransport(&benchmarkTransport{}))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := instance.DeclareTask("task", tt.opts...); !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("DeclareTask error = %v, want ErrInvalidOption", err)
			}
		})
	}
}

func TestJobOptionsAreStrictlyValidated(t *testing.T) {
	tests := []struct {
		name string
		opts []JobOption
	}{
		{name: "negative delay", opts: []JobOption{WithDelay(-time.Second)}},
		{name: "delay and run-at", opts: []JobOption{WithDelay(time.Second), WithRunAt(time.Now().Add(time.Hour))}},
		{name: "nil option", opts: []JobOption{nil}},
		{name: "empty header key", opts: []JobOption{WithHeader("", "value")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance, err := Open(DefaultConfig(), WithTransport(&benchmarkTransport{}))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := instance.DeclareTask("task"); err != nil {
				t.Fatalf("DeclareTask: %v", err)
			}
			if err := instance.Enqueue(context.Background(), "task", "payload", tt.opts...); !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("Enqueue error = %v, want ErrInvalidOption", err)
			}
		})
	}
}

func TestCodecRejectsInvalidTopologyMetadata(t *testing.T) {
	codec := NewCodec(nil)
	if _, _, err := codec.EncodeTopology(TaskTopology, "", nil, 1); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("empty topology error = %v, want ErrInvalidName", err)
	}
	if _, _, err := codec.EncodeTopology(TopologyKind(99), "task", nil, 1); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("invalid kind error = %v, want ErrInvalidOption", err)
	}
	if _, _, err := codec.EncodeTopology(TaskTopology, "task", nil, -1); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("negative max attempts error = %v, want ErrInvalidOption", err)
	}
}

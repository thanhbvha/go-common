package nats

import (
	"math"
	"testing"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestValidateTopologyRejectsInvalidNATSOptions(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value any
	}{
		{name: "max age zero", key: "nats.maxage", value: time.Duration(0)},
		{name: "replicas zero", key: "nats.replicas", value: 0},
		{name: "replicas too high", key: "nats.replicas", value: 6},
		{name: "retention invalid", key: "nats.retention", value: gonats.RetentionPolicy(99)},
		{name: "ack wait negative", key: "nats.ack_wait", value: -time.Second},
		{name: "batch zero", key: "nats.batch_size", value: 0},
		{name: "delay batch zero", key: "nats.delay_batch_size", value: 0},
		{name: "delay batch too high", key: "nats.delay_batch_size", value: 1001},
		{name: "delay lease zero", key: "nats.delay_lease", value: time.Duration(0)},
		{name: "fetch timeout wrong type", key: "nats.fetch_timeout", value: "slow"},
		{name: "unknown", key: "nats.unknown", value: 1},
	}

	validator := &natsTransport{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topology := transport.Topology{JobType: "task", Options: map[string]any{tt.key: tt.value}}
			if err := validator.ValidateTopology(topology); err == nil {
				t.Fatal("ValidateTopology unexpectedly succeeded")
			}
		})
	}
}

func TestEffectiveCapacityUsesCoreBoundsAndRejectsOverflow(t *testing.T) {
	tests := []struct {
		name string
		sub  transport.Subscription
		want int
		err  bool
	}{
		{name: "core bounds", sub: transport.Subscription{Concurrency: 3, Prefetch: 7}, want: 10},
		{name: "topology bounds", sub: transport.Subscription{Topology: transport.Topology{Options: map[string]any{"eventbus.concurrency": 2, "eventbus.prefetch": 4}}}, want: 6},
		{name: "defaults", sub: transport.Subscription{}, want: defaultConcurrency + defaultPrefetch},
		{name: "negative", sub: transport.Subscription{Concurrency: -1}, err: true},
		{name: "overflow", sub: transport.Subscription{Concurrency: math.MaxInt, Prefetch: 1}, err: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := effectiveCapacity(tt.sub)
			if (err != nil) != tt.err || got != tt.want {
				t.Fatalf("effectiveCapacity = %d, %v; want %d, error=%v", got, err, tt.want, tt.err)
			}
		})
	}
}

func TestValidateTopologyEncodesReservedNATSNameAndRejectsUnsafeLegacyName(t *testing.T) {
	validator := &natsTransport{}
	if err := validator.ValidateTopology(transport.Topology{Namespace: "a.b", JobType: "bad name/一"}); err != nil {
		t.Fatalf("v2 encoded topology rejected: %v", err)
	}
	legacy := transport.Topology{JobType: "bad name", Options: map[string]any{
		transport.ResourceNameVersionOption: transport.ResourceNameVersionLegacy,
	}}
	if err := validator.ValidateTopology(legacy); err == nil {
		t.Fatal("unsafe legacy topology unexpectedly accepted")
	}
}

func TestConsumerBatchSizeIsBoundedByCoreCapacity(t *testing.T) {
	sub := transport.Subscription{
		Topology:    transport.Topology{Options: map[string]any{"nats.batch_size": 100}},
		Concurrency: 3, Prefetch: 5,
	}
	if got := consumerBatchSize(sub); got != 8 {
		t.Fatalf("consumer batch size = %d, want 8", got)
	}
}

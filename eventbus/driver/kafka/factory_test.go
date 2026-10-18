package kafka

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
	commonkafka "github.com/thanhbvha/go-common/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNewWithConfigPropagatesFactoryToInternalConsumers(t *testing.T) {
	cfg := commonkafka.DefaultConfig()
	cfg.Brokers = []string{"broker-a:9093", "broker-b:9093"}
	cfg.ClientID = "eventbus-kafka"
	cfg.SecurityProtocol = commonkafka.SASLSSL
	cfg.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13}
	cfg.SASLMechanism = commonkafka.SASLPlain
	cfg.SASLUsername = "synthetic-user"
	cfg.SASLPassword = "synthetic-password"
	cfg.DialTimeout = 8 * time.Second
	cfg.ReadTimeout = 12 * time.Second

	created, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	backend := created.(*kafkaTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	if !backend.producerOwned || backend.factory == nil || backend.adm == nil {
		t.Fatal("secure constructor did not initialize owned producer/admin/factory")
	}

	consumer, err := backend.newConsumer(
		kgo.ConsumerGroup("eventbus-group"),
		kgo.ConsumeTopics("eventbus-topic"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if got := consumer.OptValue(kgo.ClientID); got != cfg.ClientID {
		t.Fatalf("consumer client ID = %v, want %q", got, cfg.ClientID)
	}
	if got := consumer.OptValue(kgo.SeedBrokers); len(got.([]string)) != 2 {
		t.Fatalf("consumer brokers = %#v", got)
	}
	if consumer.OptValue(kgo.DialTLSConfig) == nil || consumer.OptValue(kgo.SASL) == nil {
		t.Fatal("internal consumer did not inherit TLS/SASL")
	}
	if got := consumer.OptValue(kgo.DialTimeout); got != cfg.DialTimeout {
		t.Fatalf("consumer dial timeout = %v", got)
	}
	if got := consumer.OptValue(kgo.RequestTimeoutOverhead); got != cfg.ReadTimeout {
		t.Fatalf("consumer request timeout = %v", got)
	}
	reader, err := backend.newReader(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		"eventbus-dlq": {0: kgo.NewOffset().AtStart()},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.OptValue(kgo.DialTLSConfig) == nil || reader.OptValue(kgo.SASL) == nil {
		t.Fatal("internal DLQ reader did not inherit TLS/SASL")
	}
	if got := reader.OptValue(kgo.ConsumerGroup); got != "" {
		t.Fatalf("DLQ reader consumer group = %#v, want empty", got)
	}
}

func TestMainConsumerOverridesFactoryAutoCommit(t *testing.T) {
	cfg := commonkafka.DefaultConfig()
	if cfg.ConsumerCommitInterval <= 0 {
		t.Fatal("test requires the default auto-commit policy")
	}
	factory, err := commonkafka.NewNativeClientFactory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := factory.NewManualConsumer(mainConsumerOptions("jobs", "workers", transport.Topology{})...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if got := client.OptValue(kgo.DisableAutoCommit); got != true {
		t.Fatalf("DisableAutoCommit = %#v, want true", got)
	}
	if got := client.OptValue(kgo.AutoCommitMarks); got != false {
		t.Fatalf("AutoCommitMarks = %#v, want false", got)
	}
}

func TestDelayConsumerOverridesFactoryAutoCommit(t *testing.T) {
	cfg := commonkafka.DefaultConfig()
	factory, err := commonkafka.NewNativeClientFactory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := factory.NewManualConsumer(
		kgo.ConsumerGroup("delay-workers"),
		kgo.ConsumeTopics("jobs.delay-5s"),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if got := client.OptValue(kgo.DisableAutoCommit); got != true {
		t.Fatalf("DisableAutoCommit = %#v, want true", got)
	}
}

func TestLegacyNewCopiesNativeSecurityOptions(t *testing.T) {
	cfg := commonkafka.DefaultConfig()
	cfg.Brokers = []string{"secure:9093"}
	cfg.ClientID = "legacy-secure"
	cfg.SecurityProtocol = commonkafka.SASLSSL
	cfg.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13}
	cfg.SASLMechanism = commonkafka.SASLPlain
	cfg.SASLUsername = "synthetic-user"
	cfg.SASLPassword = "synthetic-password"
	factory, err := commonkafka.NewNativeClientFactory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	base, err := factory.NewProducer()
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	backend := New(base, nil).(*kafkaTransport)
	consumer, err := backend.newConsumer(kgo.ConsumerGroup("legacy-group"), kgo.ConsumeTopics("legacy-topic"))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if consumer.OptValue(kgo.DialTLSConfig) == nil || consumer.OptValue(kgo.SASL) == nil {
		t.Fatal("legacy adapter dropped native TLS/SASL options")
	}
	if got := consumer.OptValue(kgo.ClientID); got != cfg.ClientID {
		t.Fatalf("legacy consumer client ID = %v", got)
	}
	manual, err := backend.newManualConsumer(kgo.ConsumerGroup("legacy-manual"), kgo.ConsumeTopics("legacy-topic"))
	if err != nil {
		t.Fatal(err)
	}
	defer manual.Close()
	if manual.OptValue(kgo.DialTLSConfig) == nil || manual.OptValue(kgo.SASL) == nil {
		t.Fatal("legacy manual consumer dropped native TLS/SASL options")
	}
	if got := manual.OptValue(kgo.DisableAutoCommit); got != true {
		t.Fatalf("legacy manual consumer DisableAutoCommit = %#v, want true", got)
	}
	reader, err := backend.newReader(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		"legacy-dlq": {0: kgo.NewOffset().AtStart()},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.OptValue(kgo.DialTLSConfig) == nil || reader.OptValue(kgo.SASL) == nil {
		t.Fatal("legacy direct reader dropped native TLS/SASL options")
	}
	if got := reader.OptValue(kgo.ConsumerGroup); got != "" {
		t.Fatalf("legacy direct reader consumer group = %#v, want empty", got)
	}
}

func TestDefaultTransportWithoutRegisteredClientReturnsError(t *testing.T) {
	previous, hadPrevious := commonkafka.LookupDefault()
	commonkafka.SetDefault(nil)
	t.Cleanup(func() {
		if hadPrevious {
			commonkafka.SetDefault(previous)
		} else {
			commonkafka.SetDefault(nil)
		}
	})
	if _, err := newDefaultTransport(); err == nil {
		t.Fatal("default transport unexpectedly succeeded without a registered client")
	}
}

func TestNewWithClientRejectsDisconnectedClient(t *testing.T) {
	if _, err := NewWithClient(commonkafka.New(commonkafka.DefaultConfig())); err == nil {
		t.Fatal("NewWithClient unexpectedly accepted a disconnected client")
	}
}

func TestNewWithConfigAcceptsManualCommitPolicy(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		cfg := commonkafka.DefaultConfig()
		cfg.ConsumerCommitInterval = interval
		backend, err := NewWithConfig(cfg)
		if err != nil {
			t.Fatalf("manual commit policy: %v", err)
		}
		if err := backend.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

package kafka

import (
	"context"
	"crypto/tls"
	"errors"
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

func TestNewWithConfigRejectsUnsupportedCommitPolicy(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		cfg := commonkafka.DefaultConfig()
		cfg.ConsumerCommitInterval = interval
		backend, err := NewWithConfig(cfg)
		if backend != nil || !errors.Is(err, transport.ErrNotSupported) {
			t.Fatalf("unsupported commit policy: transport=%T error=%v", backend, err)
		}
	}
}

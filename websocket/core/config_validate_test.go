package core

import (
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
)

type realPubSub struct{ *pubsub.NoopManager }

func newTestGateway(t *testing.T) *routing.Gateway {
	t.Helper()
	gateway, err := routing.NewGateway(routing.GatewayConfig{Registry: &roomIORegistry{}, Transport: roomIOTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

func TestValidateRejectsInconsistentTimings(t *testing.T) {
	config := DefaultManagerConfig()
	if err := config.Validate(); err != nil {
		t.Fatalf("defaults rejected: %v", err)
	}
	config.PingPeriod = config.PongWait
	if err := config.Validate(); err == nil {
		t.Fatal("PingPeriod >= PongWait accepted")
	}
	config = DefaultManagerConfig()
	config.WriteWait = -time.Second
	if err := config.Validate(); err == nil {
		t.Fatal("negative WriteWait accepted")
	}
}

func TestValidateRejectsMessageLargerThanByteBurst(t *testing.T) {
	config := DefaultManagerConfig()
	config.MaxMessageBytes = 8 * 1024
	config.ByteBurst = 4 * 1024
	if err := config.Validate(); err == nil {
		t.Fatal("MaxMessageBytes > ByteBurst accepted; such messages could never pass the byte limiter")
	}
}

func TestValidateRejectsNegativeLimits(t *testing.T) {
	config := DefaultManagerConfig()
	config.MaxConnections = -1
	if err := config.Validate(); err == nil {
		t.Fatal("negative MaxConnections accepted")
	}
}

func TestProductionRejectsLegacyClusterPubSub(t *testing.T) {
	config := DefaultManagerConfig()
	config.Authorizer = denyAuthorizer{}
	config.PubSub = realPubSub{pubsub.NewNoopManager()}
	if err := config.ValidateProduction(); err == nil {
		t.Fatal("legacy pub/sub fan-out accepted without a distributed gateway")
	}
	config.PubSub = pubsub.NewNoopManager()
	if err := config.ValidateProduction(); err != nil {
		t.Fatalf("single-node production config rejected: %v", err)
	}
}

func TestProductionRejectsPubSubAlongsideGateway(t *testing.T) {
	config := DefaultManagerConfig()
	config.Authorizer = denyAuthorizer{}
	config.PubSub = realPubSub{pubsub.NewNoopManager()}
	config.Distributed = newTestGateway(t)
	if err := config.ValidateProduction(); err == nil {
		t.Fatal("PubSub silently ignored next to Distributed")
	}
}

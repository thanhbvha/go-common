package core

import (
	"bytes"
	"context"
	"testing"

	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
)

type idleWire struct{}

func (idleWire) Ready() bool { return true }
func (idleWire) Subscribe(context.Context, string, func([]byte)) (func(), error) {
	return func() {}, nil
}
func (idleWire) Publish(context.Context, string, []byte) error { return nil }

func gatewayWith(t *testing.T, options ...routing.RPCOption) *routing.Gateway {
	t.Helper()
	gateway, err := routing.NewGateway(routing.GatewayConfig{
		Registry:  &roomIORegistry{},
		Transport: routing.NewRPCTransport(idleWire{}, "production", options...),
	})
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

func authenticator(t *testing.T, mode pubsub.AuthMode) routing.MessageAuthenticator {
	t.Helper()
	a, err := pubsub.NewAuthenticator("k", map[string][]byte{"k": bytes.Repeat([]byte{5}, 32)}, mode)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func clusterConfig(gateway *routing.Gateway) ManagerConfig {
	config := DefaultManagerConfig()
	config.Authorizer = denyAuthorizer{}
	config.Distributed = gateway
	return config
}

func TestProductionClusterRequiresAuthenticatedBroker(t *testing.T) {
	cases := []struct {
		name    string
		gateway *routing.Gateway
		allow   bool
		wantErr bool
	}{
		{"unsigned transport", gatewayWith(t), false, true},
		{"passive mode still accepts forgeries", gatewayWith(t, routing.WithMessageAuthenticator(authenticator(t, pubsub.AuthPassive))), false, true},
		{"optional mode still accepts forgeries", gatewayWith(t, routing.WithMessageAuthenticator(authenticator(t, pubsub.AuthOptional))), false, true},
		{"required mode", gatewayWith(t, routing.WithMessageAuthenticator(authenticator(t, pubsub.AuthRequired))), false, false},
		{"unsigned but broker is the trust boundary", gatewayWith(t), true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := clusterConfig(tc.gateway)
			config.AllowUnauthenticatedBroker = tc.allow
			if err := config.ValidateProduction(); (err != nil) != tc.wantErr {
				t.Fatalf("ValidateProduction() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestNewProductionManagerRefusesUnsafeConfig(t *testing.T) {
	if manager, err := NewProductionManager(DefaultManagerConfig()); err == nil {
		_ = manager.Shutdown(context.Background())
		t.Fatal("manager started without an authorizer")
	}
	config := DefaultManagerConfig()
	config.Authorizer = denyAuthorizer{}
	config.MaxMessageBytes, config.ByteBurst = 1<<20, 1024
	if manager, err := NewProductionManager(config); err == nil {
		_ = manager.Shutdown(context.Background())
		t.Fatal("manager started with inconsistent limits")
	}
}

func TestNewProductionManagerStartsValidSingleNodeConfig(t *testing.T) {
	config := DefaultManagerConfig()
	config.Authorizer = denyAuthorizer{}
	manager, err := NewProductionManager(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	if !manager.IsAcceptingConnections() {
		t.Fatal("production manager is not accepting connections")
	}
}

func TestNewProductionManagerStartsAuthenticatedCluster(t *testing.T) {
	gateway := gatewayWith(t, routing.WithMessageAuthenticator(authenticator(t, pubsub.AuthRequired)))
	manager, err := NewProductionManager(clusterConfig(gateway))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
}

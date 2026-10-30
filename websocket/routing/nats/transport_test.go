package nats_test

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/websocket/routing"
	"github.com/thanhbvha/go-common/websocket/routing/contracttest"
	driver "github.com/thanhbvha/go-common/websocket/routing/nats"
	registry "github.com/thanhbvha/go-common/websocket/routing/redis"
	"testing"
	"time"
)

func TestContract(t *testing.T) {
	contracttest.Run(t, func(t *testing.T) (routing.SessionRegistry, func() routing.DeliveryTransport) {
		s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: server.RANDOM_PORT, NoLog: true, NoSigs: true})
		if err != nil {
			t.Fatal(err)
		}
		go s.Start()
		if !s.ReadyForConnections(5 * time.Second) {
			t.Fatal("NATS startup timeout")
		}
		t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
		nc, err := gonats.Connect(s.ClientURL())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		redis := miniredis.RunT(t)
		client := goredis.NewClient(&goredis.Options{Addr: redis.Addr()})
		t.Cleanup(func() { client.Close() })
		return registry.NewRegistry(client, "test"), func() routing.DeliveryTransport { return driver.NewTransport(nc, "test") }
	})
}

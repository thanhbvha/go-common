//go:build integration

package mongodb_test

import (
	"github.com/thanhbvha/go-common/v2/db/messaging/contracttest"
	brokercontract "github.com/thanhbvha/go-common/v2/db/messaging/contracttest/eventbus"
	driver "github.com/thanhbvha/go-common/v2/db/messaging/driver/mongodb"
	database "github.com/thanhbvha/go-common/v2/db/mongodb"
	"os"
	"testing"
)

func TestMongoDBMessagingEventBus(t *testing.T) {
	names := []string{"nats"}
	if os.Getenv("MESSAGING_BROKERS_REQUIRED") == "1" {
		names = []string{"redis", "nats", "kafka"}
	}
	brokercontract.Run(t, func(t *testing.T) contracttest.Fixture[*database.Transaction] {
		f := newFixture(t)
		cfg := f.config
		cfg.Namespace = f.db.Name()
		store, err := driver.New(t.Context(), f.manager, cfg)
		if err != nil {
			t.Fatal(err)
		}
		f.Namespace = cfg.Namespace
		f.Store = store
		f.Inbox = store
		f.Append = store.Append
		return f.Fixture
	}, names...)
}

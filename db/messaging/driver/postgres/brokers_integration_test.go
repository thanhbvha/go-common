//go:build integration

package postgres_test

import (
	"github.com/thanhbvha/go-common/v2/db/messaging/contracttest"
	brokercontract "github.com/thanhbvha/go-common/v2/db/messaging/contracttest/eventbus"
	"github.com/thanhbvha/go-common/v2/db/messaging/driver/postgres"
	"github.com/thanhbvha/go-common/v2/db/orm"
	"testing"
)

func TestPostgresEventBus(t *testing.T) {
	brokercontract.Run(t, func(t *testing.T) contracttest.Fixture[*orm.Transaction] {
		f := newFixture(t)
		store, err := postgres.New(t.Context(), f.client, postgres.Config{Namespace: f.schema, Schema: f.schema})
		if err != nil {
			t.Fatal(err)
		}
		f.Namespace = f.schema
		f.Store = store
		f.Inbox = store
		f.Append = store.Append
		return f.Fixture
	})
}

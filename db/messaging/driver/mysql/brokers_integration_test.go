//go:build integration

package mysql_test

import (
	"github.com/thanhbvha/go-common/v2/db/messaging/contracttest"
	brokercontract "github.com/thanhbvha/go-common/v2/db/messaging/contracttest/eventbus"
	"github.com/thanhbvha/go-common/v2/db/messaging/driver/mysql"
	"github.com/thanhbvha/go-common/v2/db/orm"
	"testing"
)

func TestMySQLMessagingEventBus(t *testing.T) {
	brokercontract.Run(t, func(t *testing.T) contracttest.Fixture[*orm.Transaction] {
		f := newFixture(t)
		store, err := mysql.New(t.Context(), f.client, mysql.Config{Namespace: f.schema, Database: f.schema})
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

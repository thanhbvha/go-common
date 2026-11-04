//go:build integration

package mysql_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/thanhbvha/go-common/v2/db/messaging"
	mysql "github.com/thanhbvha/go-common/v2/db/messaging/driver/mysql"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	"github.com/thanhbvha/go-common/v2/db/orm"
	backend "github.com/thanhbvha/go-common/v2/db/orm/backend/mysql"
)

func testMessage(f fixture, id string) messaging.Message {
	return messaging.Message{ID: id, Namespace: f.Namespace, TenantID: "tenant", Kind: messaging.Task, Type: "created", SchemaVersion: 1, Payload: []byte(`{"n":9223372036854775807}`)}
}

func TestMySQLMessagingSnapshotRegression(t *testing.T) {
	f := newFixture(t)
	ctx := t.Context()
	m := testMessage(f, "snapshot")
	if err := f.Within(ctx, func(tx *orm.Transaction) error {
		db, err := tx.DB()
		if err != nil {
			return err
		}
		// Establish the default REPEATABLE READ snapshot before the winning insert.
		var count int64
		if err := db.Raw("SELECT COUNT(*) FROM `" + f.schema + "`.outbox").Scan(&count).Error; err != nil {
			return err
		}
		if err := f.Within(ctx, func(other *orm.Transaction) error { return f.Append(other, m) }); err != nil {
			return err
		}
		return f.Append(tx, m)
	}); err != nil {
		t.Fatal("append used stale caller snapshot", err)
	}
}

func TestMySQLMessagingBinaryIdentityAndTimeRegression(t *testing.T) {
	f := newFixture(t, func(cfg *orm.Config) {
		native, err := driver.ParseDSN(os.Getenv("ORM_MYSQL_DSN"))
		if err != nil {
			t.Fatal("invalid fixture DSN")
		}
		zone, err := time.LoadLocation("Asia/Ho_Chi_Minh")
		if err != nil {
			t.Fatal(err)
		}
		native.Loc = zone
		if native.Params == nil {
			native.Params = map[string]string{}
		}
		native.Params["time_zone"] = "'+07:00'"
		cfg.Backend = backend.New(native.FormatDSN())
	})
	ctx := t.Context()
	for _, id := range []string{"Case", "case", "café", "cafe"} {
		m := testMessage(f, id)
		m.Headers = map[string]string{"$route.a": "retained"}
		m.AvailableAt = time.Date(2, 1, 1, 0, 0, 0, 123456000, time.UTC)
		if err := f.Within(ctx, func(tx *orm.Transaction) error { return f.Append(tx, m) }); err != nil {
			t.Fatal(err)
		}
		record, err := f.Store.Get(ctx, m.TenantID, id)
		if err != nil || record.Message.ID != id || !record.Message.AvailableAt.Equal(m.AvailableAt) || record.Message.Headers["$route.a"] != "retained" {
			t.Fatal("identity or schedule changed", record, err)
		}
		if time.Since(record.CreatedAt) < -time.Second || time.Since(record.CreatedAt) > time.Minute {
			t.Fatal("database/client time zone altered UTC", record.CreatedAt)
		}
	}
	claims, err := f.Store.Claim(ctx, outbox.ClaimRequest{Limit: 10, MaxAttempts: 20, Lease: time.Minute})
	if err != nil || len(claims) != 4 {
		t.Fatal(claims, err)
	}
	for _, claim := range claims {
		if time.Until(claim.LeaseUntil) < 50*time.Second || time.Until(claim.LeaseUntil) > 61*time.Second {
			t.Fatal("lease changed by DSN location", claim.LeaseUntil)
		}
		if err := f.Store.MarkPublished(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMySQLMessagingLeaseWaitRegression(t *testing.T) {
	f := newFixture(t)
	ctx := t.Context()
	m := testMessage(f, "lock-wait")
	if err := f.Within(ctx, func(tx *orm.Transaction) error { return f.Append(tx, m) }); err != nil {
		t.Fatal(err)
	}
	claims, err := f.Store.Claim(ctx, outbox.ClaimRequest{Limit: 1, MaxAttempts: 20, Lease: 500 * time.Millisecond})
	if err != nil || len(claims) != 1 {
		t.Fatal(claims, err)
	}
	settled := make(chan error, 1)
	if err := f.Within(ctx, func(tx *orm.Transaction) error {
		db, err := tx.DB()
		if err != nil {
			return err
		}
		var seq int64
		if err := db.Raw("SELECT seq FROM `"+f.schema+"`.outbox WHERE message_id=? FOR UPDATE", m.ID).Scan(&seq).Error; err != nil {
			return err
		}
		go func() { settled <- f.Store.MarkPublished(ctx, claims[0]) }()
		timer := time.NewTimer(time.Until(claims[0].LeaseUntil) + 100*time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-settled; !errors.Is(err, messaging.ErrLeaseLost) {
		t.Fatal("settlement authorized by stale statement clock", err)
	}
}

func TestMySQLMessagingSchemaAndCancelRegression(t *testing.T) {
	f := newFixture(t)
	ctx := t.Context()
	s := f.Store.(*mysql.Store)
	m := testMessage(f, "cancel")
	hash, _ := m.Fingerprint()
	receipt := inbox.Receipt{Identity: m.Identity(), Consumer: "consumer", Fingerprint: hash}
	canceled, cancel := context.WithCancel(ctx)
	result, err := s.Consume(canceled, receipt, func(tx *orm.Transaction) error {
		if err := f.Effect(tx, "cancel"); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if err == nil || result != "" || f.Count(ctx, "cancel") != 0 {
		t.Fatal("canceled handler committed", result, err)
	}
	result, err = s.Consume(ctx, receipt, func(tx *orm.Transaction) error { return f.Effect(tx, "cancel") })
	if err != nil || result != inbox.Applied {
		t.Fatal("inbox persisted after cancel", result, err)
	}
	if err := f.client.DB().Exec("ALTER TABLE `" + f.schema + "`.outbox DROP INDEX outbox_ready").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := mysql.New(ctx, f.client, mysql.Config{Namespace: f.Namespace, Database: f.schema}); !errors.Is(err, messaging.ErrSchema) {
		t.Fatal("missing claim index accepted", err)
	}
}

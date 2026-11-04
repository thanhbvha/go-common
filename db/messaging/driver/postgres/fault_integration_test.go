//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/driver/postgres"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	"github.com/thanhbvha/go-common/v2/db/orm"
	ormbackend "github.com/thanhbvha/go-common/v2/db/orm/backend"
	"gorm.io/gorm"
)

// Commit really succeeds on PostgreSQL, then its acknowledgement is lost at
// the GORM pool boundary. No production fault hook or global state is needed.
type faultBackend struct {
	ormbackend.Backend
	lose *atomic.Bool
}

func (b faultBackend) Prepare(ctx context.Context) (gorm.Dialector, error) {
	d, e := b.Backend.Prepare(ctx)
	if e != nil {
		return nil, e
	}
	return faultDialector{d, b.lose}, nil
}

type faultDialector struct {
	gorm.Dialector
	lose *atomic.Bool
}

func (d faultDialector) Initialize(db *gorm.DB) error {
	if e := d.Dialector.Initialize(db); e != nil {
		return e
	}
	pool, e := db.DB()
	if e != nil {
		return e
	}
	db.ConnPool = &faultPool{pool, d.lose}
	return nil
}

type faultPool struct {
	*sql.DB
	lose *atomic.Bool
}

func (p *faultPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	tx, e := p.DB.BeginTx(ctx, opts)
	if e != nil {
		return nil, e
	}
	return &faultTx{tx, p.lose}, nil
}
func (p *faultPool) GetDBConn() (*sql.DB, error) { return p.DB, nil }

type faultTx struct {
	*sql.Tx
	lose *atomic.Bool
}

func (t *faultTx) Commit() error {
	if e := t.Tx.Commit(); e != nil {
		return e
	}
	if t.lose.CompareAndSwap(true, false) {
		return errors.New("fixture lost commit response")
	}
	return nil
}

func TestCommitUnknownReconciliation(t *testing.T) {
	flag := new(atomic.Bool)
	f := newFixture(t, func(cfg *orm.Config) { cfg.Backend = faultBackend{cfg.Backend, flag} })
	s := f.Store.(*postgres.Store)
	ctx := t.Context()
	m := messaging.Message{ID: "unknown", Namespace: f.Namespace, Kind: messaging.Task, Type: "created", SchemaVersion: 1, Payload: []byte(`{}`)}
	flag.Store(true)
	err := f.Within(ctx, func(tx *orm.Transaction) error {
		if e := f.Effect(tx, "producer"); e != nil {
			return e
		}
		return s.Append(tx, m)
	})
	if !errors.Is(err, orm.ErrCommitUnknown) || f.Count(ctx, "producer") != 1 {
		t.Fatal("producer commit unknown not preserved", err)
	}
	if _, e := s.Get(ctx, "", m.ID); e != nil {
		t.Fatal("committed outbox not visible", e)
	}
	hash, _ := m.Fingerprint()
	r := inbox.Receipt{Identity: m.Identity(), Consumer: "consumer", Fingerprint: hash}
	flag.Store(true)
	result, err := s.Consume(ctx, r, func(tx *orm.Transaction) error { return f.Effect(tx, "consumer") })
	if result != "" || !errors.Is(err, orm.ErrCommitUnknown) || f.Count(ctx, "consumer") != 1 {
		t.Fatal("inbox commit acknowledged success", result, err)
	}
	result, err = s.Consume(ctx, r, func(*orm.Transaction) error { t.Error("unknown commit replay applied twice"); return nil })
	if err != nil || result != inbox.Duplicate {
		t.Fatal(result, err)
	}
	req := outbox.ClaimRequest{Limit: 1, MaxAttempts: 20, Lease: time.Minute}
	flag.Store(true)
	claims, err := s.Claim(ctx, req)
	if !errors.Is(err, orm.ErrCommitUnknown) || len(claims) != 0 {
		t.Fatal("ambiguous claim escaped", claims, err)
	}
	f.Expire(ctx, m.Identity())
	claims, err = s.Claim(ctx, req)
	if err != nil || len(claims) != 1 {
		t.Fatal(claims, err)
	}
	flag.Store(true)
	err = s.MarkPublished(ctx, claims[0])
	if !errors.Is(err, orm.ErrCommitUnknown) {
		t.Fatal(err)
	}
	record, err := s.Get(ctx, "", m.ID)
	if err != nil || record.State != outbox.Published {
		t.Fatal(record, err)
	}
}

func TestBusinessConstraintIsNotInboxDuplicate(t *testing.T) {
	f := newFixture(t)
	ctx := t.Context()
	m := messaging.Message{ID: "constraint", Namespace: f.Namespace, Kind: messaging.Event, Type: "created", SchemaVersion: 1, Payload: []byte(`{}`)}
	hash, _ := m.Fingerprint()
	receipt := inbox.Receipt{Identity: m.Identity(), Consumer: "consumer", Fingerprint: hash}
	if err := f.Within(ctx, func(tx *orm.Transaction) error { return f.Effect(tx, "unique") }); err != nil {
		t.Fatal(err)
	}
	_, err := f.Inbox.Consume(ctx, receipt, func(tx *orm.Transaction) error {
		db, e := tx.DB()
		if e != nil {
			return e
		}
		return db.Exec(`INSERT INTO "` + f.schema + `".effects VALUES ('unique',1)`).Error
	})
	if err == nil {
		t.Fatal("business unique violation acknowledged")
	}
	result, err := f.Inbox.Consume(ctx, receipt, func(tx *orm.Transaction) error { return f.Effect(tx, "recovered") })
	if err != nil || result != inbox.Applied || f.Count(ctx, "recovered") != 1 {
		t.Fatal("inbox survived failed handler", result, err)
	}
}

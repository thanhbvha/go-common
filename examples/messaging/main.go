// This executable owns an isolated PostgreSQL schema or MongoDB databases and
// uses selected EventBus broker fixtures. It never mutates application tables.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/thanhbvha/go-common/v2/db/messaging"
	consumer "github.com/thanhbvha/go-common/v2/db/messaging/consumer/eventbus"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	publisher "github.com/thanhbvha/go-common/v2/db/messaging/publisher/eventbus"
	"github.com/thanhbvha/go-common/v2/db/orm"
	bus "github.com/thanhbvha/go-common/v2/eventbus"
)

func main() {
	if err := run(); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, failureMessage(err))
		os.Exit(1)
	}
}

type responseLoss struct {
	publisher outbox.Publisher
	drop      atomic.Bool
	calls     atomic.Int32
}

func (p *responseLoss) Publish(ctx context.Context, m messaging.Message) error {
	p.calls.Add(1)
	if err := p.publisher.Publish(ctx, m); err != nil {
		return err
	}
	if p.drop.CompareAndSwap(true, false) {
		return context.DeadlineExceeded
	}
	return nil
}
func run() error {
	cfg, err := parseConfig(os.Args[1:], os.Stderr)
	if err != nil {
		return &stepError{step: "configuration", err: err, hint: "check -config path, YAML field types, driver/broker selection and environment overrides"}
	}
	drivers, _ := selectDrivers(cfg.Driver)
	brokers, _ := selectBrokers(cfg.Broker)
	for _, driver := range drivers {
		for _, broker := range brokers {
			if err := runSelectedBroker(context.Background(), cfg, driver, broker, os.Stdout); err != nil {
				return err
			}
		}
	}
	return nil
}

func selectDrivers(name string) ([]string, error) {
	switch name {
	case "all":
		return []string{"postgres", "mysql", "mongodb"}, nil
	case "postgres", "mysql", "mongodb":
		return []string{name}, nil
	default:
		return nil, errors.New("driver must be postgres, mysql, mongodb, or all")
	}
}
func (cfg exampleConfig) validateDriver(name string) error {
	switch name {
	case "postgres":
		if strings.TrimSpace(cfg.Postgres.DSN) == "" {
			return errors.New("postgres.dsn or ORM_POSTGRES_DSN required for disposable PostgreSQL")
		}
	case "mysql":
		if strings.TrimSpace(cfg.MySQL.DSN) == "" {
			return errors.New("mysql.dsn or ORM_MYSQL_DSN required for disposable MySQL 8.4/InnoDB")
		}
	case "mongodb":
		if strings.TrimSpace(cfg.MongoDB.URI) == "" {
			return errors.New("mongodb.uri or MONGODB_INTEGRATION_URI required for disposable MongoDB replica set")
		}
	default:
		return errors.New("select one supported database driver per fixture")
	}
	return nil
}
func runSelectedBroker(ctx context.Context, cfg exampleConfig, driver, broker string, output io.Writer) error {
	if err := cfg.validateDriver(driver); err != nil {
		return err
	}
	if driver == "mongodb" {
		return runMongoDBBroker(ctx, cfg, broker, output)
	}
	return runSQLBroker(ctx, cfg, driver, broker, output)
}

func runBroker(parent context.Context, settings exampleConfig, broker string, output io.Writer) error {
	return runSQLBroker(parent, settings, "postgres", broker, output)
}
func runSQLBroker(parent context.Context, settings exampleConfig, driver, broker string, output io.Writer) (result error) {
	if err := settings.validateDriver(driver); err != nil {
		return err
	}
	if err := settings.validateBroker(broker); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	defer cancel()
	fixture, err := newSQLFixture(ctx, settings, driver)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, fixture.close()) }()
	client, store, schema, prefix := fixture.client, fixture.store, fixture.namespace, fixture.prefix
	// Application phase: the fixture factory owns broker clients, while the bus
	// owns its transport runtime. Cleanup runs after the bus has drained.
	tr, cleanup, err := newBroker(ctx, settings, broker)
	if err != nil {
		return err
	}
	defer cleanup()
	bcfg := bus.DefaultConfig()
	bcfg.Namespace = schema
	b, err := bus.Open(bcfg, bus.WithTransport(tr))
	if err != nil {
		return err
	}
	defer b.Close(context.Background())
	var calls atomic.Int32
	handler, err := consumer.WrapInbox(store, consumer.Config{Namespace: schema, Consumer: "orders-projector", Tenant: func(context.Context, bus.Job) (string, error) { return "demo", nil }}, func(_ context.Context, tx *orm.Transaction, job bus.Job) error {
		var payload struct {
			OrderID string `json:"order_id"`
		}
		if err := job.Decode(&payload); err != nil {
			return err
		}
		db, err := tx.DB()
		if err != nil {
			return err
		}
		if err := db.Exec("INSERT INTO "+prefix+"effects VALUES (?)", payload.OrderID).Error; err != nil {
			return err
		}
		calls.Add(1)
		return nil
	})
	if err != nil {
		return err
	}
	if err := b.DeclareTask("order.created", bus.WithDelayDisabled()); err != nil {
		return err
	}
	if err := b.HandleTask("order.created", handler); err != nil {
		return err
	}
	if err := b.Start(ctx); err != nil {
		return err
	}
	id := uuid.NewString()
	message := messaging.Message{ID: id, Namespace: schema, TenantID: "demo", Kind: messaging.Task, Type: "order.created", SchemaVersion: 1, Payload: []byte(`{"order_id":"` + id + `"}`)}
	rolledBack := message
	rolledBack.ID = uuid.NewString()
	sentinel := errors.New("fixture rollback")
	if err := client.WithTransaction(ctx, nil, func(tx *orm.Transaction) error {
		db, err := tx.DB()
		if err != nil {
			return err
		}
		if err := db.Exec("INSERT INTO "+prefix+"orders VALUES (?)", rolledBack.ID).Error; err != nil {
			return err
		}
		if err := store.Append(tx, rolledBack); err != nil {
			return err
		}
		return sentinel
	}); !errors.Is(err, sentinel) {
		return errors.New("producer rollback failed")
	}
	if _, err := store.Get(ctx, "demo", rolledBack.ID); !errors.Is(err, messaging.ErrNotFound) {
		return errors.New("rolled-back outbox persisted")
	}
	var rolledBackOrders int64
	if err := client.DB().WithContext(ctx).Raw("SELECT count(*) FROM "+prefix+"orders WHERE id=?", rolledBack.ID).Scan(&rolledBackOrders).Error; err != nil {
		return err
	}
	if rolledBackOrders != 0 {
		return errors.New("rolled-back business write persisted")
	}
	if err := client.WithTransaction(ctx, nil, func(tx *orm.Transaction) error {
		db, err := tx.DB()
		if err != nil {
			return err
		}
		if err := db.Exec("INSERT INTO "+prefix+"orders VALUES (?)", id).Error; err != nil {
			return err
		}
		return store.Append(tx, message)
	}); err != nil {
		return err
	}
	// Simulate a crashed process after claim, before publication.
	claims, err := store.Claim(ctx, outbox.ClaimRequest{Limit: 1, MaxAttempts: 20, Lease: 10 * time.Millisecond})
	if err != nil || len(claims) != 1 {
		return errors.New("claim fixture failed")
	}
	time.Sleep(20 * time.Millisecond)
	publisher, err := publisher.NewPublisher(b, publisher.Config{Namespace: schema})
	if err != nil {
		return err
	}
	fault := &responseLoss{publisher: publisher}
	fault.drop.Store(true)
	dcfg := outbox.DefaultConfig()
	dcfg.PollInterval = 10 * time.Millisecond
	dcfg.RetryMin = 10 * time.Millisecond
	dcfg.RetryMax = 20 * time.Millisecond
	dispatcher, err := outbox.NewDispatcher(store, fault, dcfg)
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- dispatcher.Run(ctx) }()
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = dispatcher.Drain(cleanup)
		<-done
	}()
	for {
		record, err := store.Get(ctx, "demo", id)
		if err != nil {
			return err
		}
		if record.State == outbox.Published && calls.Load() == 1 {
			fingerprint, _ := message.Fingerprint()
			result, err := store.Consume(ctx, inbox.Receipt{Identity: message.Identity(), Consumer: "orders-projector", Fingerprint: fingerprint}, func(*orm.Transaction) error { return errors.New("duplicate handler invoked") })
			if err != nil || result != inbox.Duplicate {
				return errors.New("inbox replay failed")
			}
			var counts struct{ Orders, Effects int64 }
			if err := client.DB().WithContext(ctx).Raw("SELECT (SELECT count(*) FROM " + prefix + "orders) AS orders,(SELECT count(*) FROM " + prefix + "effects) AS effects").Scan(&counts).Error; err != nil {
				return err
			}
			if counts.Orders != 1 || counts.Effects != 1 || record.Attempts != 3 || fault.calls.Load() != 2 {
				return errors.New("committed recovery assertions failed")
			}
			fmt.Fprintf(output, "driver=%s; broker=%s; rollback=verified; order + outbox committed; recovered claim; claim attempts=%d; publish calls=%d; business effects=%d; inbox replay=%s\n", driver, broker, record.Attempts, fault.calls.Load(), counts.Effects, result)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

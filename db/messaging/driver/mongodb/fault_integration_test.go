//go:build integration

package mongodb_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	driver "github.com/thanhbvha/go-common/v2/db/messaging/driver/mongodb"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	database "github.com/thanhbvha/go-common/v2/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func failCommand(t *testing.T, f fixture, mode any, data bson.M) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := f.db.Client().Database("admin").RunCommand(ctx, bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: mode}, {Key: "data", Value: data}}).Err(); err != nil {
		t.Fatal("configure isolated failpoint failed")
	}
	disable := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.db.Client().Database("admin").RunCommand(ctx, bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"}}).Err(); err != nil {
			t.Error("disable failpoint failed")
		}
	}
	t.Cleanup(disable)
	return disable
}
func TestMongoDBMessagingTransactionRetries(t *testing.T) {
	f := newFixture(t)
	m := message(f, "retry")
	fp, _ := m.Fingerprint()
	receipt := inbox.Receipt{Identity: m.Identity(), Consumer: "consumer", Fingerprint: fp}
	calls := 0
	result, err := f.store.Consume(t.Context(), receipt, func(tx *database.Transaction) error {
		calls++
		if err := f.Effect(tx, "retry"); err != nil {
			return err
		}
		if calls == 1 {
			return mongo.CommandError{Code: 112, Message: "fixture write conflict", Labels: []string{"TransientTransactionError"}}
		}
		return f.Append(tx, message(f, "next"))
	})
	if err != nil || result != inbox.Applied || calls != 2 || f.Count(t.Context(), "retry") != 1 {
		t.Fatal("native callback retry changed business result", result, calls, err)
	}
	if _, err := f.store.Get(t.Context(), "a", "next"); err != nil {
		t.Fatal(err)
	}
}
func TestMongoDBMessagingCommitResponseLoss(t *testing.T) {
	if os.Getenv("MONGODB_TEST_FAILPOINTS") != "1" {
		if os.Getenv("MESSAGING_MONGODB_REQUIRED") == "1" {
			t.Fatal("commit fault fixture required")
		}
		t.Skip("enable isolated server failpoints")
	}
	t.Run("commit_retry_without_callback_replay", func(t *testing.T) {
		f := newFixture(t)
		m := message(f, "commit")
		fp, _ := m.Fingerprint()
		receipt := inbox.Receipt{Identity: m.Identity(), Consumer: "consumer", Fingerprint: fp}
		failCommand(t, f, bson.M{"times": 1}, bson.M{"failCommands": bson.A{"commitTransaction"}, "writeConcernError": bson.M{"code": 64, "errmsg": "fixture lost commit acknowledgement"}, "errorLabels": bson.A{"UnknownTransactionCommitResult"}})
		calls := 0
		result, err := f.store.Consume(t.Context(), receipt, func(tx *database.Transaction) error { calls++; return f.Effect(tx, "commit") })
		if err != nil || result != inbox.Applied || calls != 1 || f.Count(t.Context(), "commit") != 1 {
			t.Fatal(result, calls, err)
		}
	})
	t.Run("unresolved_commit_never_acknowledged", func(t *testing.T) {
		f := newFixture(t, func(cfg *database.Config) { cfg.OperationTimeout = 300 * time.Millisecond })
		m := message(f, "unknown")
		fp, _ := m.Fingerprint()
		receipt := inbox.Receipt{Identity: m.Identity(), Consumer: "consumer", Fingerprint: fp}
		disable := failCommand(t, f, "alwaysOn", bson.M{"failCommands": bson.A{"commitTransaction"}, "writeConcernError": bson.M{"code": 64, "errmsg": "fixture ambiguous commit"}, "errorLabels": bson.A{"UnknownTransactionCommitResult"}})
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		calls := 0
		result, err := f.store.Consume(ctx, receipt, func(tx *database.Transaction) error { calls++; return f.Effect(tx, "unknown") })
		disable()
		if err == nil || result != "" || calls != 1 {
			t.Fatal("unknown commit acknowledged", result, calls, err)
		}
		if f.Count(t.Context(), "unknown") != 1 {
			t.Fatal("fixture did not commit before response loss")
		}
		result, err = f.store.Consume(t.Context(), receipt, func(*database.Transaction) error {
			t.Error("unknown commit redelivery reran business effect")
			return nil
		})
		if err != nil || result != inbox.Duplicate {
			t.Fatal(result, err)
		}
	})
	t.Run("late_commit_after_cancellation_never_acknowledged", func(t *testing.T) {
		f := newFixture(t, func(cfg *database.Config) { cfg.OperationTimeout = time.Second })
		m := message(f, "late")
		fp, _ := m.Fingerprint()
		r := inbox.Receipt{Identity: m.Identity(), Consumer: "consumer", Fingerprint: fp}
		failCommand(t, f, bson.M{"times": 1}, bson.M{"failCommands": bson.A{"commitTransaction"}, "blockConnection": true, "blockTimeMS": 250})
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		result, err := f.store.Consume(ctx, r, func(tx *database.Transaction) error { return f.Effect(tx, "late") })
		if result != "" || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("late commit acknowledged", result, err)
		}
		if f.Count(t.Context(), "late") != 1 {
			t.Fatal("fixture did not commit late")
		}
		result, err = f.store.Consume(t.Context(), r, func(*database.Transaction) error { t.Error("late commit applied twice"); return nil })
		if err != nil || result != inbox.Duplicate {
			t.Fatal(result, err)
		}
	})
}
func TestMongoDBMessagingStandaloneRejected(t *testing.T) {
	uri := os.Getenv("MONGODB_STANDALONE_URI")
	if uri == "" {
		return
	} // Secure profile has no standalone fixture.
	manager := database.NewManager()
	if err := manager.Init(t.Context(), map[string]database.Config{"main": {URI: uri, DBName: "messaging_standalone_rejection"}}); err != nil {
		t.Fatal("standalone fixture startup failed")
	}
	defer manager.DisconnectAll(context.Background())
	// Detection rejects the deployment before provisioning any collections.
	if err := driver.Provision(t.Context(), manager, driver.Config{Namespace: "test", Alias: "main"}); !errors.Is(err, database.ErrTransactionsUnsupported) {
		t.Fatal("standalone accepted", err)
	}
}

func TestMongoDBMessagingPrimaryStepdown(t *testing.T) {
	if os.Getenv("MONGODB_TEST_FAILOVER") != "1" {
		return
	} // Three-node replica fixture only.
	f := newFixture(t)
	m := message(f, "stepdown")
	appendMessage(t, f, m)
	claims, err := f.store.Claim(t.Context(), outbox.ClaimRequest{Limit: 1, MaxAttempts: 20, Lease: time.Minute})
	if err != nil || len(claims) != 1 {
		t.Fatal(claims, err)
	}
	fault, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	_ = f.db.Client().Database("admin").RunCommand(fault, bson.D{{Key: "replSetStepDown", Value: 5}, {Key: "force", Value: true}}).Err()
	cancel()
	ctx, stop := context.WithTimeout(t.Context(), 30*time.Second)
	defer stop()
	// Majority ownership must survive election. If settlement acknowledgement is
	// ambiguous, reconcile the durable state before attempting settlement again.
	for {
		err = f.store.MarkPublished(ctx, claims[0])
		record, readErr := f.store.Get(ctx, m.TenantID, m.ID)
		if readErr == nil && record.State == outbox.Published {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("publication state did not recover after stepdown", err, readErr)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestMongoDBMessagingAmbiguousClaim(t *testing.T) {
	if os.Getenv("MONGODB_TEST_FAILPOINTS") != "1" {
		if os.Getenv("MESSAGING_MONGODB_REQUIRED") == "1" {
			t.Fatal("claim fault fixture required")
		}
		t.Skip("enable isolated failpoints")
	}
	f := newFixture(t)
	m := message(f, "ambiguous-claim")
	appendMessage(t, f, m)
	// An unconfirmed write may or may not have executed. No claims may escape
	// either way; recovery must reconcile the durable state, not assume rollback.
	disable := failCommand(t, f, bson.M{"times": 1}, bson.M{"failCommands": bson.A{"findAndModify"}, "writeConcernError": bson.M{"code": 64, "errmsg": "fixture lost claim acknowledgement"}, "errorLabels": bson.A{}})
	req := outbox.ClaimRequest{Limit: 1, MaxAttempts: 3, Lease: 500 * time.Millisecond}
	claims, err := f.store.Claim(t.Context(), req)
	disable()
	if err == nil || len(claims) != 0 {
		t.Fatal("ambiguous ownership returned", claims, err)
	}
	record, err := f.store.Get(t.Context(), m.TenantID, m.ID)
	if err != nil || (record.State != outbox.Processing && record.State != outbox.Pending) {
		t.Fatal("unexpected state after claim acknowledgement loss", record, err)
	}
	// A majority read can still show the pre-write version while the ambiguous
	// write catches up. Wait for its lease, rather than inferring non-ownership
	// from that observation or modifying bookkeeping based on a stale read.
	time.Sleep(req.Lease + 100*time.Millisecond)
	req.Lease = time.Minute
	claims, err = f.store.Claim(t.Context(), req)
	if err != nil || len(claims) != 1 || claims[0].Message.ID != m.ID || claims[0].Attempt < 1 || claims[0].Attempt > 2 {
		t.Fatal(claims, err)
	}
	if err := f.store.MarkPublished(t.Context(), claims[0]); err != nil {
		t.Fatal(err)
	}
}

//go:build integration

package mongodb_test

import (
	"context"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoDBProductionIntegration(t *testing.T) {
	uri := os.Getenv("MONGODB_INTEGRATION_URI")
	if uri == "" {
		t.Skip("set MONGODB_INTEGRATION_URI to a dedicated test deployment")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	name := "go_common_it_" + bson.NewObjectID().Hex()
	cfg := mongodb.Config{URI: uri, DBName: name, RequireTransactions: true, AppName: "go-common-integration"}
	m := mongodb.NewManager()
	if err := m.Init(ctx, map[string]mongodb.Config{"main": cfg}); err != nil {
		t.Fatal("production integration startup failed (connection details omitted)")
	}
	db, err := m.Lookup("main")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := db.Drop(cleanup); err != nil {
			t.Error("temporary database cleanup failed")
		}
		if err := m.DisconnectAll(cleanup); err != nil {
			t.Error("client cleanup failed")
		}
	})
	if caps, err := m.Capabilities(ctx, "main"); err != nil || !caps.Transactions {
		t.Fatalf("transaction capability not detected: %v", err)
	}

	t.Run("cursor_pagination_and_streaming_aggregation", func(t *testing.T) {
		r := mongodb.NewRepository[integrationDocument](db, "cursor_items")
		rows := make([]integrationDocument, 17)
		for i := range rows {
			rows[i] = integrationDocument{ID: i + 1, Score: i + 1, Group: "fixture"}
		}
		if _, err := r.InsertManyTyped(ctx, rows); err != nil {
			t.Fatal(err)
		}
		for _, descending := range []bool{false, true} {
			after, expected, count := "", 1, 0
			if descending {
				expected = 17
			}
			for {
				page, err := r.PaginateCursor(ctx, bson.M{"group": "fixture"}, mongodb.CursorPageRequest{Size: 4, After: after, Descending: descending}, options.Find().SetBatchSize(2).SetSkip(99).SetLimit(0))
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range page.Items {
					if row.ID != expected {
						t.Fatalf("cursor skipped/duplicated an ID: %d != %d", row.ID, expected)
					}
					if descending {
						expected--
					} else {
						expected++
					}
					count++
				}
				if !page.HasMore {
					if page.NextCursor != "" {
						t.Fatal("terminal page has cursor")
					}
					break
				}
				after = page.NextCursor
			}
			if count != 17 {
				t.Fatalf("wrong cursor count: %d", count)
			}
		}
		sum := 0
		if err := r.AggregateEach(ctx, mongo.Pipeline{bson.D{{Key: "$match", Value: bson.M{"group": "fixture"}}}}, func(_ context.Context, row *integrationDocument) error { sum += row.Score; return nil }, options.Aggregate().SetBatchSize(2)); err != nil || sum != 153 {
			t.Fatalf("aggregate sum=%d: %v", sum, err)
		}
		stop := errors.New("expected aggregate stop")
		if err := r.AggregateEach(ctx, mongo.Pipeline{}, func(context.Context, *integrationDocument) error { return stop }, options.Aggregate().SetBatchSize(1)); !errors.Is(err, stop) {
			t.Fatalf("aggregate callback error lost: %v", err)
		}
	})

	t.Run("scoped_options_array_filters_and_unordered_writes", func(t *testing.T) {
		type element struct {
			Score int `bson:"score"`
		}
		type document struct {
			ID     int       `bson:"_id"`
			Values []element `bson:"values"`
		}
		r := mongodb.NewRepository[document](db, "option_items")
		if _, err := r.InsertOneWithOptions(ctx, &document{ID: 1, Values: []element{{1}, {2}}}); err != nil {
			t.Fatal(err)
		}
		err := m.WithTransactionAcross(ctx, []string{"main"}, func(tx *mongodb.Transaction) error {
			scoped, err := mongodb.NewTransactionRepository[document](tx, "main", "option_items")
			if err != nil {
				return err
			}
			_, err = scoped.UpdateOneWithOptions(tx.Context(), bson.M{"_id": 1}, bson.M{"$inc": bson.M{"values.$[v].score": 10}}, options.UpdateOne().SetArrayFilters([]any{bson.M{"v.score": bson.M{"$gte": 2}}}))
			if err != nil {
				return err
			}
			if _, err := scoped.UpdateOneWithOptions(ctx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"values": bson.A{}}}); !errors.Is(err, mongodb.ErrTransactionContext) {
				t.Fatal("options bypassed context guard")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.FindByIDWithOptions(ctx, 1)
		if err != nil || got.Values[0].Score != 1 || got.Values[1].Score != 12 {
			t.Fatalf("array filters failed: %v", err)
		}
		_, err = r.InsertManyWithOptions(ctx, []any{document{ID: 2}, document{ID: 2}, document{ID: 3}}, options.InsertMany().SetOrdered(false))
		if !mongo.IsDuplicateKeyError(err) {
			t.Fatal("unordered duplicate not reported")
		}
		if n, err := r.CountWithOptions(ctx, bson.M{}); err != nil || n != 3 {
			t.Fatalf("unordered write stopped early: %d %v", n, err)
		}
	})

	t.Run("shutdown_waits_for_real_commit", func(t *testing.T) {
		owner := mongodb.NewManager()
		if err := owner.Init(ctx, map[string]mongodb.Config{"main": cfg}); err != nil {
			t.Fatal("shutdown fixture startup failed")
		}
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_ = owner.DisconnectAll(cleanup)
		})
		if err := db.CreateCollection(ctx, "drain_records"); err != nil {
			t.Fatal(err)
		}
		entered, finish := make(chan struct{}), make(chan struct{})
		transactionDone := make(chan error, 1)
		go func() {
			transactionDone <- owner.WithTransactionAcross(ctx, []string{"main"}, func(tx *mongodb.Transaction) error {
				r, err := mongodb.NewTransactionRepository[integrationDocument](tx, "main", "drain_records")
				if err != nil {
					return err
				}
				if _, err := r.InsertOne(tx.Context(), &integrationDocument{ID: 1}); err != nil {
					return err
				}
				close(entered)
				select {
				case <-finish:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}()
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("transaction did not start")
		}
		short, stop := context.WithTimeout(ctx, 30*time.Millisecond)
		err := owner.DisconnectAll(short)
		stop()
		close(finish)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("drain did not wait: %v", err)
		}
		if err := <-transactionDone; err != nil {
			t.Fatal(err)
		}
		if err := owner.DisconnectAll(ctx); err != nil {
			t.Fatal(err)
		}
		if n, err := mongodb.NewRepository[integrationDocument](db, "drain_records").Count(ctx, bson.M{}); err != nil || n != 1 {
			t.Fatalf("drain interrupted commit: %d %v", n, err)
		}
	})

	if os.Getenv("MONGODB_TEST_FAILPOINTS") == "1" {
		t.Run("unknown_commit_retries_commit_without_callback_replay", func(t *testing.T) {
			if err := db.CreateCollection(ctx, "commit_retry"); err != nil {
				t.Fatal(err)
			}
			// Inject acknowledgement failure after execution, without pretending the
			// primary is shutting down (which would test topology recovery instead).
			configureFailCommand(t, db, bson.M{"failCommands": bson.A{"commitTransaction"}, "writeConcernError": bson.M{"code": 64, "errmsg": "synthetic commit acknowledgement failure"}, "errorLabels": bson.A{"UnknownTransactionCommitResult"}})
			calls := 0
			err := m.WithTransaction(ctx, "main", func(sessCtx context.Context) error {
				calls++
				_, err := mongodb.NewRepository[integrationDocument](db, "commit_retry").InsertOne(sessCtx, &integrationDocument{ID: 1})
				return err
			})
			if err != nil || calls != 1 {
				t.Fatalf("commit retry replayed business callback: %d %v", calls, err)
			}
			if n, err := mongodb.NewRepository[integrationDocument](db, "commit_retry").Count(ctx, bson.M{}); err != nil || n != 1 {
				t.Fatalf("commit outcome incorrect: %d %v", n, err)
			}
		})
	}
	if os.Getenv("MONGODB_TEST_FAILOVER") == "1" {
		t.Run("primary_stepdown_retries_transaction", func(t *testing.T) {
			if err := db.CreateCollection(ctx, "stepdown_retry"); err != nil {
				t.Fatal(err)
			}
			r := mongodb.NewRepository[integrationDocument](db, "stepdown_retry")
			calls := 0
			err := m.WithTransaction(ctx, "main", func(sessCtx context.Context) error {
				calls++
				if _, err := r.InsertOne(sessCtx, &integrationDocument{ID: 1}); err != nil {
					return err
				}
				if calls == 1 {
					fault, stop := context.WithTimeout(ctx, 5*time.Second)
					_ = db.Client().Database("admin").RunCommand(fault, bson.D{{Key: "replSetStepDown", Value: 5}, {Key: "force", Value: true}}).Err()
					stop()
				}
				_, err := r.UpdateOne(sessCtx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"score": 1}})
				return err
			})
			if err != nil || calls < 2 {
				t.Fatalf("primary failover callback attempts=%d err=%v", calls, err)
			}
			if n, err := r.Count(ctx, bson.M{}); err != nil || n != 1 {
				t.Fatalf("failover duplicated write: %d %v", n, err)
			}
		})
	}
	if os.Getenv("MONGODB_TEST_SECURITY") == "1" {
		t.Run("TLS_and_authentication_enforced", func(t *testing.T) {
			parsed, err := url.Parse(uri)
			if err != nil {
				t.Fatal("bad fixture URI")
			}
			for _, scenario := range []string{"wrong-password", "untrusted-CA", "plaintext"} {
				modified := *parsed
				q := modified.Query()
				switch scenario {
				case "wrong-password":
					modified.User = url.UserPassword(parsed.User.Username(), "invalid-test-password")
				case "untrusted-CA":
					q.Del("tlsCAFile")
				case "plaintext":
					q.Del("tlsCAFile")
					q.Set("tls", "false")
				}
				modified.RawQuery = q.Encode()
				bad := cfg
				bad.URI = modified.String()
				bad.PingTimeout = time.Second
				client, err := mongodb.New(ctx, bad)
				if err == nil {
					_ = client.Disconnect(ctx)
					t.Fatalf("security control not enforced: %s", scenario)
				}
			}
		})
	}
	if standalone := os.Getenv("MONGODB_STANDALONE_URI"); standalone != "" {
		t.Run("standalone_rejected_before_transaction_callback", func(t *testing.T) {
			bad := cfg
			bad.URI = standalone
			client, err := mongodb.New(ctx, bad)
			if client != nil {
				_ = client.Disconnect(ctx)
			}
			if !errors.Is(err, mongodb.ErrTransactionsUnsupported) {
				t.Fatal("standalone not rejected with typed capability error")
			}
		})
	}
}

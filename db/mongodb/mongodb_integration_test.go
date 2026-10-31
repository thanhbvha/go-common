//go:build integration

package mongodb_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type integrationDocument struct {
	ID    int    `bson:"_id"`
	Code  string `bson:"code"`
	Score int    `bson:"score"`
	Group string `bson:"group"`
}

// TestMongoDBIntegration writes only synthetic data in a fresh database. Never
// point this suite at production. Failpoint cases require a dedicated test server.
func TestMongoDBIntegration(t *testing.T) {
	uri := os.Getenv("MONGODB_INTEGRATION_URI")
	if uri == "" {
		t.Skip("set MONGODB_INTEGRATION_URI to an isolated MongoDB replica set")
	}
	dbName := "go_common_it_" + bson.NewObjectID().Hex()
	peerDBName := "go_common_it_" + bson.NewObjectID().Hex()
	cfg := mongodb.Config{URI: uri, DBName: dbName, AppName: "go-common-integration", PingTimeout: 5 * time.Second}
	m := mongodb.NewManager()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := m.Init(ctx, map[string]mongodb.Config{"main": cfg}); err != nil {
		t.Fatal("could not connect to integration MongoDB (URI and credentials omitted)")
	}
	db, err := m.Lookup("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		// Both names are generated above, never taken from the URI or environment.
		for _, name := range []string{dbName, peerDBName} {
			if !strings.HasPrefix(name, "go_common_it_") || len(name) != len("go_common_it_")+24 {
				t.Error("refusing unsafe database cleanup")
				return
			}
			if err := db.Client().Database(name).Drop(cleanup); err != nil {
				t.Errorf("temporary database cleanup failed: %v", err)
			}
		}
		if err := m.DisconnectAll(cleanup); err != nil {
			t.Errorf("client cleanup failed: %v", err)
		}
	})
	var hello struct {
		SetName string `bson:"setName"`
	}
	if err := db.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		t.Fatal(err)
	}
	if hello.SetName == "" {
		t.Fatal("integration suite requires a replica set for transaction checks")
	}
	var build struct {
		Version string `bson:"version"`
	}
	if err := db.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&build); err != nil {
		t.Fatal(err)
	}
	t.Logf("MongoDB %s; dedicated temporary database; replica set available", build.Version)

	newRepo := func(t *testing.T, count int) *mongodb.Repository[integrationDocument] {
		t.Helper()
		repo := mongodb.NewRepository[integrationDocument](db, "items_"+bson.NewObjectID().Hex())
		if count > 0 {
			docs := make([]interface{}, count)
			for i := range docs {
				docs[i] = integrationDocument{ID: i + 1, Code: fmt.Sprintf("item-%04d", i+1), Score: i + 1, Group: "fixture"}
			}
			if _, err := repo.InsertMany(ctx, docs); err != nil {
				t.Fatal(err)
			}
		}
		return repo
	}

	t.Run("CRUD_and_not_found", func(t *testing.T) {
		r := newRepo(t, 3)
		if got, err := r.FindByID(ctx, 1); err != nil || got.Score != 1 {
			t.Fatalf("FindByID: %v, %v", got, err)
		}
		if _, err := r.FindByID(ctx, 99); !errors.Is(err, mongodb.ErrNotFound) {
			t.Fatalf("not-found contract: %v", err)
		}
		if got, err := r.FindOne(ctx, bson.M{"code": "item-0002"}); err != nil || got.ID != 2 {
			t.Fatalf("FindOne: %v, %v", got, err)
		}
		if ok, err := r.Exists(ctx, nil); err != nil || !ok {
			t.Fatalf("Exists: %v, %v", ok, err)
		}
		if ok, err := r.Exists(ctx, bson.M{"_id": 99}); err != nil || ok {
			t.Fatalf("missing Exists: %v, %v", ok, err)
		}
		if _, err := r.Exists(ctx, bson.M{"$invalidOperator": 1}); err == nil {
			t.Fatal("invalid query was reported as missing")
		}
		result, err := r.UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$inc": bson.M{"score": 10}})
		if err != nil || result.ModifiedCount != 1 {
			t.Fatalf("UpdateOne: %v, %v", result, err)
		}
		updated, err := r.FindOneAndUpdate(ctx, bson.M{"_id": 1}, bson.M{"$inc": bson.M{"score": 1}}, options.FindOneAndUpdate().SetReturnDocument(options.After))
		if err != nil || updated.Score != 12 {
			t.Fatalf("FindOneAndUpdate: %v, %v", updated, err)
		}
		upsert, err := r.UpsertOne(ctx, bson.M{"_id": 4}, bson.M{"$set": bson.M{"score": 4, "code": "item-0004"}})
		if err != nil || upsert.UpsertedCount != 1 {
			t.Fatalf("UpsertOne: %v, %v", upsert, err)
		}
		many, err := r.UpdateMany(ctx, bson.M{"group": "fixture"}, bson.M{"$set": bson.M{"group": "updated"}})
		if err != nil || many.ModifiedCount != 3 {
			t.Fatalf("UpdateMany: %v, %v", many, err)
		}
		removed, err := r.FindOneAndDelete(ctx, bson.M{"_id": 4})
		if err != nil || removed.ID != 4 {
			t.Fatalf("FindOneAndDelete: %v, %v", removed, err)
		}
		if _, err := r.FindOneAndDelete(ctx, bson.M{"_id": 99}); !errors.Is(err, mongodb.ErrNotFound) {
			t.Fatalf("delete not-found: %v", err)
		}
		deleted, err := r.DeleteOne(ctx, bson.M{"_id": 1})
		if err != nil || deleted.DeletedCount != 1 {
			t.Fatalf("DeleteOne: %v, %v", deleted, err)
		}
		all, err := r.DeleteMany(ctx, bson.M{})
		if err != nil || all.DeletedCount != 2 {
			t.Fatalf("DeleteMany: %v, %v", all, err)
		}
		empty, err := r.FindAll(ctx, bson.M{})
		if err != nil || empty == nil || len(empty) != 0 {
			t.Fatalf("empty list: %v, %v", empty, err)
		}
	})

	t.Run("ObjectID_roundtrip", func(t *testing.T) {
		type document struct {
			ID    bson.ObjectID `bson:"_id,omitempty"`
			Value string        `bson:"value"`
		}
		r := mongodb.NewRepository[document](db, "object_ids")
		inserted, err := r.InsertOne(ctx, &document{Value: "synthetic"})
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.FindByID(ctx, inserted.InsertedID)
		if err != nil || got.ID.IsZero() || got.Value != "synthetic" {
			t.Fatalf("ObjectID roundtrip: %v, %v", got, err)
		}
	})

	t.Run("unique_index_bulk_and_aggregation", func(t *testing.T) {
		r := newRepo(t, 3)
		if _, err := r.CreateIndex(ctx, mongo.IndexModel{Keys: bson.D{{Key: "code", Value: 1}}, Options: options.Index().SetUnique(true)}); err != nil {
			t.Fatal(err)
		}
		_, err := r.InsertOne(ctx, &integrationDocument{ID: 4, Code: "item-0001"})
		if !mongo.IsDuplicateKeyError(err) {
			t.Fatalf("unique constraint not enforced: %v", err)
		}
		bulk, err := r.BulkWrite(ctx, []mongo.WriteModel{
			mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": 1}).SetUpdate(bson.M{"$inc": bson.M{"score": 10}}),
			mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": 3}),
		})
		if err != nil || bulk.ModifiedCount != 1 || bulk.DeletedCount != 1 {
			t.Fatalf("BulkWrite: %v, %v", bulk, err)
		}
		var groups []string
		if err := r.Distinct(ctx, "group", nil, &groups); err != nil || len(groups) != 1 || groups[0] != "fixture" {
			t.Fatalf("Distinct: %v, %v", groups, err)
		}
		var sums []struct {
			Total int `bson:"total"`
		}
		err = r.Aggregate(ctx, mongo.Pipeline{bson.D{{Key: "$group", Value: bson.M{"_id": nil, "total": bson.M{"$sum": "$score"}}}}}, &sums)
		if err != nil || len(sums) != 1 || sums[0].Total != 13 {
			t.Fatalf("Aggregate: %v, %v", sums, err)
		}
	})

	t.Run("pagination_bounds_sort_projection_and_empty_pages", func(t *testing.T) {
		r := newRepo(t, 1205)
		page, err := r.Paginate(ctx, nil, mongodb.PageRequest{Page: 2, Size: 10}, options.Find().SetLimit(0).SetSkip(0))
		if err != nil {
			t.Fatal(err)
		}
		if page.TotalRows != 1205 || page.TotalPages != 121 || len(page.Items) != 10 || page.Items[0].ID != 11 {
			t.Fatalf("incorrect bounded page: %+v", page)
		}
		capped, err := r.Paginate(ctx, nil, mongodb.PageRequest{Size: 5000}, options.Find().SetLimit(0))
		if err != nil || capped.Size != 1000 || len(capped.Items) != 1000 {
			t.Fatalf("page cap bypassed: %v", err)
		}
		custom, err := r.Paginate(ctx, nil, mongodb.PageRequest{Size: 2}, options.Find().SetSort(bson.D{{Key: "score", Value: -1}, {Key: "_id", Value: -1}}).SetProjection(bson.M{"_id": 1}))
		if err != nil || custom.Items[0].ID != 1205 || custom.Items[0].Code != "" {
			t.Fatalf("sort/projection: %v", err)
		}
		empty, err := r.Paginate(ctx, bson.M{"score": -1}, mongodb.PageRequest{})
		if err != nil || empty.TotalRows != 0 || empty.TotalPages != 0 || empty.Items == nil || len(empty.Items) != 0 {
			t.Fatalf("empty pagination: %v", err)
		}
		last, err := r.PaginateSlice(ctx, nil, mongodb.PageRequest{Page: 121, Size: 10})
		if err != nil || last.HasMore || len(last.Items) != 5 || last.Items[0].ID != 1201 {
			t.Fatalf("last slice page: %v", err)
		}
		more, err := r.PaginateSlice(ctx, nil, mongodb.PageRequest{Size: 10}, options.Find().SetLimit(0))
		if err != nil || !more.HasMore || len(more.Items) != 10 {
			t.Fatalf("slice page: %v", err)
		}
	})

	t.Run("streaming_batches_stop_and_cancellation", func(t *testing.T) {
		r := newRepo(t, 137)
		seen := make(map[int]bool)
		err := r.FindEach(ctx, nil, func(_ context.Context, doc *integrationDocument) error { seen[doc.ID] = true; return nil }, options.Find().SetBatchSize(7))
		if err != nil || len(seen) != 137 {
			t.Fatalf("streamed %d documents: %v", len(seen), err)
		}
		stop := errors.New("expected callback stop")
		calls := 0
		err = r.FindEach(ctx, nil, func(context.Context, *integrationDocument) error { calls++; return stop }, options.Find().SetBatchSize(2))
		if !errors.Is(err, stop) || calls != 1 {
			t.Fatalf("callback stop: calls=%d, %v", calls, err)
		}
		canceled, stopContext := context.WithCancel(ctx)
		defer stopContext()
		err = r.FindEach(canceled, nil, func(context.Context, *integrationDocument) error { stopContext(); return context.Canceled }, options.Find().SetBatchSize(2))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("stream cancellation lost: %v", err)
		}
		if count, err := r.Count(ctx, bson.M{}); err != nil || count != 137 {
			t.Fatalf("client unusable after cancellation: %d, %v", count, err)
		}
	})

	t.Run("transaction_commit_and_rollback", func(t *testing.T) {
		r := newRepo(t, 0)
		// Precreate collection outside a transaction for compatibility with older servers.
		if err := db.CreateCollection(ctx, r.Collection().Name()); err != nil {
			t.Fatal(err)
		}
		err := m.WithTransaction(ctx, "main", func(sessCtx context.Context) error {
			_, err := r.InsertOne(sessCtx, &integrationDocument{ID: 1, Code: "committed"})
			return err
		}, options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()))
		if err != nil {
			t.Fatal(err)
		}
		if found, err := r.Exists(ctx, bson.M{"_id": 1}); err != nil || !found {
			t.Fatalf("commit missing: %v", err)
		}
		rollback := errors.New("expected business rollback")
		err = m.WithTransaction(ctx, "main", func(sessCtx context.Context) error {
			if _, err := r.InsertOne(sessCtx, &integrationDocument{ID: 2, Code: "rolled-back"}); err != nil {
				return err
			}
			if _, err := r.UpdateOne(sessCtx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"score": 100}}); err != nil {
				return err
			}
			return rollback
		})
		if !errors.Is(err, rollback) {
			t.Fatalf("rollback error lost: %v", err)
		}
		if found, err := r.Exists(ctx, bson.M{"_id": 2}); err != nil || found {
			t.Fatalf("rolled-back insert visible: %v", err)
		}
		if got, err := r.FindByID(ctx, 1); err != nil || got.Score != 0 {
			t.Fatalf("rolled-back update persisted: %v", err)
		}
	})

	t.Run("manager_rollback_shutdown_and_reinitialization", func(t *testing.T) {
		var closed atomic.Int64
		good := cfg
		good.PoolMonitor = &event.PoolMonitor{Event: func(e *event.PoolEvent) {
			if e.Type == event.ConnectionPoolClosed {
				closed.Add(1)
			}
		}}
		bad := cfg
		bad.PingTimeout = time.Nanosecond
		other := mongodb.NewManager()
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_ = other.DisconnectAll(cleanup)
		})
		if err := other.Init(ctx, map[string]mongodb.Config{"a": good, "b": bad}, "a"); err == nil {
			t.Fatal("expired ping accepted")
		}
		if _, err := other.Lookup("a"); err == nil {
			t.Fatal("failed initialization published connection")
		}
		if closed.Load() == 0 {
			t.Fatal("failed initialization did not close earlier client's pool")
		}
		if err := other.Init(ctx, map[string]mongodb.Config{"main": cfg}); err != nil {
			t.Fatal(err)
		}
		before, err := other.Lookup("")
		if err != nil {
			t.Fatal(err)
		}
		if err := other.DisconnectAll(ctx); err != nil {
			t.Fatal(err)
		}
		if err := before.Client().Ping(ctx, nil); !errors.Is(err, mongo.ErrClientDisconnected) {
			t.Fatalf("old client remained connected: %v", err)
		}
		if _, err := other.Lookup(""); err == nil {
			t.Fatal("closed handle still published")
		}
		if err := other.Init(ctx, map[string]mongodb.Config{"main": cfg}); err != nil {
			t.Fatal(err)
		}
		after, err := other.Lookup("")
		if err != nil || before.Client() == after.Client() {
			t.Fatal("reinitialization did not replace client")
		}
		if err := after.Client().Ping(ctx, nil); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("multi_database_scope_commit_rollback_and_guards", func(t *testing.T) {
		grouped, ordersDB, inventoryDB := integrationGroupedManager(t, ctx, cfg, peerDBName)
		if ordersDB.Client() != inventoryDB.Client() {
			t.Fatal("group did not reuse its client")
		}
		if err := ordersDB.CreateCollection(ctx, "tx_orders"); err != nil {
			t.Fatal(err)
		}
		if err := inventoryDB.CreateCollection(ctx, "tx_stock"); err != nil {
			t.Fatal(err)
		}
		orders := mongodb.NewRepository[integrationDocument](ordersDB, "tx_orders")
		stock := mongodb.NewRepository[integrationDocument](inventoryDB, "tx_stock")
		var retained *mongodb.Repository[integrationDocument]
		var savedContext context.Context
		writeBoth := func(tx *mongodb.Transaction, id int) error {
			a, err := mongodb.NewTransactionRepository[integrationDocument](tx, "orders", "tx_orders")
			if err != nil {
				return err
			}
			b, err := mongodb.NewTransactionRepository[integrationDocument](tx, "inventory", "tx_stock")
			if err != nil {
				return err
			}
			retained, savedContext = a, tx.Context()
			if _, err := a.InsertOne(tx.Context(), &integrationDocument{ID: id, Code: "synthetic"}); err != nil {
				return err
			}
			_, err = b.InsertOne(tx.Context(), &integrationDocument{ID: id, Code: "synthetic"})
			return err
		}
		aliases := []string{"orders", "inventory"}
		if err := grouped.WithTransactionAcross(ctx, aliases, func(tx *mongodb.Transaction) error { return writeBoth(tx, 1) }, options.Transaction().SetWriteConcern(writeconcern.Majority())); err != nil {
			t.Fatal(err)
		}
		for _, r := range []*mongodb.Repository[integrationDocument]{orders, stock} {
			if exists, err := r.Exists(ctx, bson.M{"_id": 1}); err != nil || !exists {
				t.Fatalf("cross-database commit missing: %v", err)
			}
		}
		if _, err := retained.Count(savedContext, bson.M{}); !errors.Is(err, mongodb.ErrTransactionClosed) {
			t.Fatalf("expired scope remained usable: %v", err)
		}
		rollback := errors.New("expected cross-database rollback")
		err := grouped.WithTransactionAcross(ctx, aliases, func(tx *mongodb.Transaction) error {
			if err := writeBoth(tx, 2); err != nil {
				return err
			}
			return rollback
		})
		if !errors.Is(err, rollback) {
			t.Fatalf("rollback cause lost: %v", err)
		}
		for _, r := range []*mongodb.Repository[integrationDocument]{orders, stock} {
			if exists, err := r.Exists(ctx, bson.M{"_id": 2}); err != nil || exists {
				t.Fatalf("cross-database rollback failed: %v", err)
			}
		}
		err = grouped.WithTransactionAcross(ctx, aliases, func(tx *mongodb.Transaction) error {
			a, err := mongodb.NewTransactionRepository[integrationDocument](tx, "orders", "tx_orders")
			if err != nil {
				return err
			}
			if _, err := a.InsertOne(tx.Context(), &integrationDocument{ID: 3}); err != nil {
				return err
			}
			return grouped.WithTransaction(tx.Context(), "inventory", func(context.Context) error { t.Fatal("nested callback executed"); return nil })
		})
		if !errors.Is(err, mongodb.ErrNestedTransaction) {
			t.Fatalf("nested transaction accepted: %v", err)
		}
		if exists, err := orders.Exists(ctx, bson.M{"_id": 3}); err != nil || exists {
			t.Fatalf("nested rejection did not roll back outer write: %v", err)
		}
		err = grouped.WithTransactionAcross(ctx, aliases, func(tx *mongodb.Transaction) error {
			a, err := mongodb.NewTransactionRepository[integrationDocument](tx, "orders", "tx_orders")
			if err != nil {
				return err
			}
			b, err := mongodb.NewTransactionRepository[integrationDocument](tx, "inventory", "tx_stock")
			if err != nil {
				return err
			}
			if _, err := a.InsertOne(tx.Context(), &integrationDocument{ID: 4}); err != nil {
				return err
			}
			_, err = b.InsertOne(ctx, &integrationDocument{ID: 4})
			return err // Intentional wrong context.
		})
		if !errors.Is(err, mongodb.ErrTransactionContext) {
			t.Fatalf("wrong context accepted: %v", err)
		}
		for _, r := range []*mongodb.Repository[integrationDocument]{orders, stock} {
			if exists, err := r.Exists(ctx, bson.M{"_id": 4}); err != nil || exists {
				t.Fatalf("context guard allowed partial write: %v", err)
			}
		}
		isolated := cfg
		isolated.ClientGroup = ""
		isolated.DBName = peerDBName
		if err := grouped.AddConnection(ctx, "isolated", isolated, false); err != nil {
			t.Fatal(err)
		}
		err = grouped.WithTransactionAcross(ctx, []string{"orders", "isolated"}, func(*mongodb.Transaction) error { t.Fatal("different-client callback executed"); return nil })
		if !errors.Is(err, mongodb.ErrTransactionClientMismatch) {
			t.Fatalf("different-client scope accepted: %v", err)
		}
		if err := grouped.WithTransactionAcross(ctx, []string{"orders"}, func(tx *mongodb.Transaction) error {
			_, err := tx.Database("inventory")
			if !errors.Is(err, mongodb.ErrTransactionScope) {
				t.Fatalf("undeclared alias accepted: %v", err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("concurrent_client_pool_reads_and_writes", func(t *testing.T) {
		r := newRepo(t, 1)
		const workers, operations = 8, 20
		failures := make(chan error, workers)
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < operations; j++ {
					if _, err := r.UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$inc": bson.M{"score": 1}}); err != nil {
						failures <- err
						return
					}
					if _, err := r.FindByID(ctx, 1); err != nil {
						failures <- err
						return
					}
				}
			}()
		}
		wg.Wait()
		close(failures)
		for err := range failures {
			t.Error(err)
		}
		got, err := r.FindByID(ctx, 1)
		if err != nil || got.Score != 1+workers*operations {
			t.Fatalf("concurrent increments lost: %v, %v", got, err)
		}
	})

	t.Run("live_telemetry_excludes_document_values", func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
		previous := otel.GetTracerProvider()
		otel.SetTracerProvider(provider)
		defer func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) }()
		tracedCfg := cfg
		tracedCfg.EnableTelemetry = true
		client, err := mongodb.New(ctx, tracedCfg)
		if err != nil {
			t.Fatal("traced client connection failed")
		}
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_ = client.Disconnect(cleanup)
		}()
		r := mongodb.NewRepository[integrationDocument](client.Database(dbName), "traced_documents")
		const privateValue = "synthetic-document-value-must-not-appear-in-traces"
		if _, err := r.InsertOne(ctx, &integrationDocument{ID: 1, Code: privateValue}); err != nil {
			t.Fatal(err)
		}
		if _, err := r.FindOne(ctx, bson.M{"code": privateValue}); err != nil {
			t.Fatal(err)
		}
		spans := exporter.GetSpans()
		if len(spans) < 3 {
			t.Fatalf("expected real ping/insert/find spans, got %d", len(spans))
		}
		for _, span := range spans {
			for _, attr := range span.Attributes {
				if attr.Key == "db.query.text" || attr.Key == "db.statement" || strings.Contains(attr.Value.Emit(), privateValue) {
					t.Fatal("live trace leaked document contents")
				}
			}
		}
	})

	if os.Getenv("MONGODB_TEST_FAILPOINTS") == "1" {
		t.Run("multi_database_retry_rolls_back_first_database", func(t *testing.T) {
			grouped, ordersDB, inventoryDB := integrationGroupedManager(t, ctx, cfg, peerDBName)
			if err := ordersDB.CreateCollection(ctx, "retry_orders"); err != nil {
				t.Fatal(err)
			}
			stock := mongodb.NewRepository[integrationDocument](inventoryDB, "retry_stock")
			if _, err := stock.InsertOne(ctx, &integrationDocument{ID: 1, Score: 100}); err != nil {
				t.Fatal(err)
			}
			configureFailCommand(t, ordersDB, bson.M{"failCommands": bson.A{"update"}, "errorCode": 112, "errorLabels": bson.A{"TransientTransactionError"}})
			calls := 0
			err := grouped.WithTransactionAcross(ctx, []string{"orders", "inventory"}, func(tx *mongodb.Transaction) error {
				calls++
				orders, err := mongodb.NewTransactionRepository[integrationDocument](tx, "orders", "retry_orders")
				if err != nil {
					return err
				}
				inventory, err := mongodb.NewTransactionRepository[integrationDocument](tx, "inventory", "retry_stock")
				if err != nil {
					return err
				}
				if _, err := orders.InsertOne(tx.Context(), &integrationDocument{ID: 1}); err != nil {
					return err
				}
				_, err = inventory.UpdateOne(tx.Context(), bson.M{"_id": 1}, bson.M{"$inc": bson.M{"score": -1}})
				return err
			})
			if err != nil || calls != 2 {
				t.Fatalf("cross-database retry calls=%d err=%v", calls, err)
			}
			orders := mongodb.NewRepository[integrationDocument](ordersDB, "retry_orders")
			if n, err := orders.Count(ctx, bson.M{}); err != nil || n != 1 {
				t.Fatalf("retry duplicated first DB write: count=%d err=%v", n, err)
			}
			if got, err := stock.FindByID(ctx, 1); err != nil || got.Score != 99 {
				t.Fatalf("retry applied stock update incorrectly: %v, %v", got, err)
			}
		})
		t.Run("transaction_retry_on_real_write_conflict", func(t *testing.T) {
			r := newRepo(t, 0)
			if err := db.CreateCollection(ctx, r.Collection().Name()); err != nil {
				t.Fatal(err)
			}
			configureFailCommand(t, db, bson.M{"failCommands": bson.A{"insert"}, "errorCode": 112, "errorLabels": bson.A{"TransientTransactionError"}})
			calls := 0
			err := m.WithTransaction(ctx, "", func(sessCtx context.Context) error {
				calls++
				_, err := r.InsertOne(sessCtx, &integrationDocument{ID: 1, Code: "retried"})
				return err
			})
			if err != nil || calls != 2 {
				t.Fatalf("transaction retry calls=%d, error=%v", calls, err)
			}
			if count, err := r.Count(ctx, bson.M{}); err != nil || count != 1 {
				t.Fatalf("retry duplicated write: count=%d, error=%v", count, err)
			}
		})
		t.Run("operation_deadline_and_pool_recovery", func(t *testing.T) {
			r := newRepo(t, 1)
			configureFailCommand(t, db, bson.M{"failCommands": bson.A{"find"}, "blockConnection": true, "blockTimeMS": 500})
			short, stop := context.WithTimeout(ctx, 75*time.Millisecond)
			defer stop()
			started := time.Now()
			_, err := r.FindByID(short, 1)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline not propagated: %v", err)
			}
			if time.Since(started) > 2*time.Second {
				t.Fatal("operation exceeded timeout budget")
			}
			if _, err := r.FindByID(ctx, 1); err != nil {
				t.Fatalf("pool did not recover after timeout: %v", err)
			}
		})
	} else {
		t.Log("failpoint cases disabled; set MONGODB_TEST_FAILPOINTS=1 only on a dedicated test server")
	}
}

func integrationGroupedManager(t *testing.T, ctx context.Context, cfg mongodb.Config, peerDBName string) (*mongodb.Manager, *mongo.Database, *mongo.Database) {
	t.Helper()
	manager := mongodb.NewManager()
	cfg.ClientGroup = "business"
	peer := cfg
	peer.DBName = peerDBName
	if err := manager.Init(ctx, map[string]mongodb.Config{"orders": cfg, "inventory": peer}, "orders"); err != nil {
		t.Fatal("grouped integration startup failed")
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := manager.DisconnectAll(cleanup); err != nil {
			t.Errorf("grouped client shutdown failed: %v", err)
		}
	})
	a, err := manager.Lookup("orders")
	if err != nil {
		t.Fatal(err)
	}
	b, err := manager.Lookup("inventory")
	if err != nil {
		t.Fatal(err)
	}
	return manager, a, b
}

// configureFailCommand changes server-wide behavior. It is opt-in, filtered to
// this suite's AppName, applies once, and is reset even if a subtest fails.
func configureFailCommand(t *testing.T, db *mongo.Database, data bson.M) {
	t.Helper()
	data["appName"] = "go-common-integration"
	admin := db.Client().Database("admin")
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	err := admin.RunCommand(ctx, bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: bson.M{"times": 1}}, {Key: "data", Value: data}}).Err()
	if err != nil {
		t.Fatal("failCommand requires an isolated server with enableTestCommands=1 and admin privileges")
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := admin.RunCommand(cleanup, bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"}}).Err(); err != nil {
			t.Errorf("failed to disable test failpoint: %v", err)
		}
	})
}

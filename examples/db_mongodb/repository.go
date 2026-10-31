package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/thanhbvha/go-common/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Sample struct {
	ID    int    `bson:"_id"`
	Code  string `bson:"code"`
	Score int    `bson:"score"`
	Group string `bson:"group"`
}
type Order struct {
	ID       string `bson:"_id"`
	SKU      string `bson:"sku"`
	Quantity int    `bson:"quantity"`
}
type Stock struct {
	ID        string `bson:"_id"`
	Available int    `bson:"available"`
}
type OutboxEvent struct {
	ID      string `bson:"_id"`
	Kind    string `bson:"kind"`
	OrderID string `bson:"order_id"`
}

func (s *exampleState) seed(ctx context.Context) error {
	orders, err := s.manager.Lookup("orders")
	if err != nil {
		return err
	}
	inventory, err := s.manager.Lookup("inventory")
	if err != nil {
		return err
	}
	for _, entry := range []struct{ alias, collection string }{{"orders", "orders"}, {"orders", "samples"}, {"inventory", "stock"}, {"audit", "outbox"}} {
		db, err := s.manager.Lookup(entry.alias)
		if err != nil {
			return err
		}
		// DDL belongs outside transaction callbacks.
		if err := db.CreateCollection(ctx, entry.collection); err != nil {
			return err
		}
	}
	samples := mongodb.NewRepository[Sample](orders, "samples")
	if _, err := samples.CreateIndex(ctx, mongo.IndexModel{Keys: bson.D{{Key: "code", Value: 1}}, Options: options.Index().SetUnique(true)}); err != nil {
		return err
	}
	if _, err := samples.CreateIndex(ctx, mongo.IndexModel{Keys: bson.D{{Key: "group", Value: 1}, {Key: "score", Value: -1}, {Key: "_id", Value: -1}}}); err != nil {
		return err
	}
	docs := make([]Sample, 25)
	for i := range docs {
		docs[i] = Sample{ID: i + 1, Code: fmt.Sprintf("sample-%02d", i+1), Score: i + 1, Group: "fixture"}
	}
	if _, err := samples.InsertManyTyped(ctx, docs); err != nil {
		return err
	}
	stock := mongodb.NewRepository[Stock](inventory, "stock")
	if _, err := stock.InsertOne(ctx, &Stock{ID: "demo-sku", Available: 10}); err != nil {
		return err
	}
	return nil
}

func (s *exampleState) sampleRepository() (*mongodb.Repository[Sample], error) {
	db, err := s.manager.Lookup("orders")
	if err != nil {
		return nil, err
	}
	return mongodb.NewRepository[Sample](db, "samples"), nil
}

func (s *exampleState) crud(ctx context.Context) error {
	r, err := s.sampleRepository()
	if err != nil {
		return err
	}
	if _, err := r.InsertOne(ctx, &Sample{ID: 100, Code: "crud-insert", Group: "temporary"}); err != nil {
		return err
	}
	if _, err := r.FindByID(ctx, 100); err != nil {
		return err
	}
	if _, err := r.FindOne(ctx, bson.M{"code": "crud-insert"}); err != nil {
		return err
	}
	if exists, err := r.Exists(ctx, bson.M{"_id": 100}); err != nil || !exists {
		return errors.Join(errors.New("insert not visible"), err)
	}
	if _, err := r.FindByID(ctx, 999); !errors.Is(err, mongodb.ErrNotFound) {
		return errors.New("unexpected not-found contract")
	}
	if _, err := r.InsertOne(ctx, &Sample{ID: 101, Code: "crud-insert"}); !mongo.IsDuplicateKeyError(err) {
		return errors.New("unique index did not reject duplicate")
	}
	updated, err := r.UpdateOneWithOptions(ctx, bson.M{"_id": 100}, bson.M{"$inc": bson.M{"score": 5}}, options.UpdateOne().SetComment("example-update"))
	if err != nil {
		return err
	}
	if updated.MatchedCount != 1 {
		return errors.New("update matched no document")
	}
	if _, err := r.UpsertOne(ctx, bson.M{"_id": 102}, bson.M{"$set": bson.M{"code": "crud-upsert", "group": "temporary"}}); err != nil {
		return err
	}
	after, err := r.FindOneAndUpdate(ctx, bson.M{"_id": 100}, bson.M{"$inc": bson.M{"score": 1}}, options.FindOneAndUpdate().SetReturnDocument(options.After))
	if err != nil {
		return err
	}
	if after.Score != 6 {
		return errors.New("unexpected updated value")
	}
	if _, err := r.UpdateMany(ctx, bson.M{"group": "temporary"}, bson.M{"$set": bson.M{"score": 6}}); err != nil {
		return err
	}
	if _, err := r.BulkWrite(ctx, []mongo.WriteModel{mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": 102}).SetUpdate(bson.M{"$inc": bson.M{"score": 1}})}); err != nil {
		return err
	}
	items, err := r.FindAll(ctx, bson.M{"group": "temporary"}, options.Find().SetLimit(10))
	if err != nil {
		return err
	}
	if len(items) != 2 {
		return errors.New("unexpected bounded FindAll result")
	}
	var groups []string
	if err := r.Distinct(ctx, "group", nil, &groups); err != nil {
		return err
	}
	var totals []struct {
		Count int `bson:"count"`
	}
	if err := r.Aggregate(ctx, mongo.Pipeline{bson.D{{Key: "$match", Value: bson.M{"group": "fixture"}}}, bson.D{{Key: "$count", Value: "count"}}}, &totals); err != nil {
		return err
	}
	if len(totals) != 1 || totals[0].Count != 25 {
		return errors.New("unexpected aggregate count")
	}
	if _, err := r.FindOneAndDelete(ctx, bson.M{"_id": 100}); err != nil {
		return err
	}
	if _, err := r.DeleteOne(ctx, bson.M{"_id": 102}); err != nil {
		return err
	}
	if _, err := r.DeleteMany(ctx, bson.M{"group": "temporary"}); err != nil {
		return err
	}
	fmt.Fprintln(s.out, "  CRUD, not-found, unique index, upsert, bulk, distinct and aggregation verified")
	return nil
}

func (s *exampleState) pagination(ctx context.Context) error {
	r, err := s.sampleRepository()
	if err != nil {
		return err
	}
	filter := bson.M{"group": "fixture"}
	find := options.Find().SetSort(bson.D{{Key: "score", Value: -1}, {Key: "_id", Value: -1}}).SetProjection(bson.M{"_id": 1, "score": 1})
	page, err := r.Paginate(ctx, filter, mongodb.PageRequest{Page: 2, Size: 5}, find)
	if err != nil {
		return err
	}
	if page.TotalRows != 25 || page.TotalPages != 5 || len(page.Items) != 5 || page.Items[0].Score != 20 {
		return errors.New("unexpected exact-count page")
	}
	slice, err := r.PaginateSlice(ctx, filter, mongodb.PageRequest{Page: 1, Size: 5}, find)
	if err != nil {
		return err
	}
	if !slice.HasMore || len(slice.Items) != 5 {
		return errors.New("unexpected count-free page")
	}
	fmt.Fprintf(s.out, "  exact count: rows=%d pages=%d; count-free: items=%d has_more=%v\n", page.TotalRows, page.TotalPages, len(slice.Items), slice.HasMore)
	ascendingToken := ""
	for _, descending := range []bool{false, true} {
		after, count := "", 0
		for {
			cursorPage, err := r.PaginateCursor(ctx, filter, mongodb.CursorPageRequest{Size: 7, After: after, Descending: descending}, options.Find().SetBatchSize(3).SetProjection(bson.M{"_id": 1, "score": 1}))
			if err != nil {
				return err
			}
			for _, row := range cursorPage.Items {
				expected := count + 1
				if descending {
					expected = 25 - count
				}
				if row.ID != expected || row.Code != "" {
					return errors.New("cursor ordering or projection changed")
				}
				count++
			}
			if !cursorPage.HasMore {
				if cursorPage.NextCursor != "" {
					return errors.New("terminal cursor page returned another token")
				}
				break
			}
			after = cursorPage.NextCursor
			if !descending && ascendingToken == "" {
				ascendingToken = after
			}
		}
		if count != 25 {
			return errors.New("keyset pagination skipped or duplicated documents")
		}
	}
	if _, err := r.PaginateCursor(ctx, filter, mongodb.CursorPageRequest{After: "bad%cursor"}); !errors.Is(err, mongodb.ErrInvalidCursor) {
		return errors.New("invalid cursor was accepted")
	}
	if _, err := r.PaginateCursor(ctx, filter, mongodb.CursorPageRequest{After: ascendingToken, Descending: true}); !errors.Is(err, mongodb.ErrInvalidCursor) {
		return errors.New("cursor direction mismatch was accepted")
	}
	fmt.Fprintln(s.out, "  indexed cursor pages visited 25 documents in each direction; projection and invalid tokens verified")
	return nil
}

func (s *exampleState) stream(ctx context.Context) error {
	r, err := s.sampleRepository()
	if err != nil {
		return err
	}
	count := 0
	err = r.FindEach(ctx, bson.M{"group": "fixture"}, func(_ context.Context, _ *Sample) error { count++; return nil }, options.Find().SetBatchSize(4))
	if err != nil {
		return err
	}
	if count != 25 {
		return errors.New("stream did not process every document")
	}
	stop := errors.New("expected callback stop")
	err = r.FindEach(ctx, bson.M{}, func(context.Context, *Sample) error { return stop }, options.Find().SetBatchSize(4))
	if !errors.Is(err, stop) {
		return errors.New("stream lost callback error")
	}
	aggregated := 0
	if err := r.AggregateEach(ctx, mongo.Pipeline{bson.D{{Key: "$match", Value: bson.M{"group": "fixture"}}}}, func(_ context.Context, row *Sample) error { aggregated += row.Score; return nil }, options.Aggregate().SetBatchSize(4)); err != nil {
		return err
	}
	if aggregated != 325 {
		return errors.New("streaming aggregation returned incorrect sum")
	}
	// Decode a different aggregation output type without materializing a slice.
	type Summary struct {
		Count int `bson:"count"`
	}
	summaryCount := 0
	err = mongodb.ExecuteAggregateEach[Summary](ctx, r.Collection(), mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.M{"group": "fixture"}}},
		bson.D{{Key: "$count", Value: "count"}},
	}, func(_ context.Context, row *Summary) error { summaryCount += row.Count; return nil })
	if err != nil || summaryCount != 25 {
		return errors.Join(errors.New("typed streaming aggregation returned incorrect count"), err)
	}
	err = r.AggregateEach(ctx, mongo.Pipeline{}, func(context.Context, *Sample) error { return stop }, options.Aggregate().SetBatchSize(1))
	if !errors.Is(err, stop) {
		return errors.New("aggregation stream lost callback error")
	}
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	err = r.AggregateEach(canceled, mongo.Pipeline{}, func(context.Context, *Sample) error { cancel(); return nil }, options.Aggregate().SetBatchSize(1))
	if !errors.Is(err, context.Canceled) {
		return errors.New("aggregation stream ignored cancellation")
	}
	fmt.Fprintf(s.out, "  streamed %d documents across batches; callback stop propagated\n", count)
	fmt.Fprintln(s.out, "  aggregation sum=325 typed summary=25; callback stop and cancellation propagated")
	return nil
}

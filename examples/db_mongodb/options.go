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

type OptionLine struct {
	Score int `bson:"score"`
}
type OptionDocument struct {
	ID    int          `bson:"_id"`
	Lines []OptionLine `bson:"lines"`
}

func (s *exampleState) driverOptions(ctx context.Context) error {
	db, err := s.manager.Lookup("orders")
	if err != nil {
		return err
	}
	r := mongodb.NewRepository[OptionDocument](db, "option_documents")
	if _, err := r.InsertManyTyped(ctx, []OptionDocument{{ID: 1, Lines: []OptionLine{{Score: 1}, {Score: 2}}}}); err != nil {
		return err
	}
	err = s.manager.WithTransactionAcross(ctx, []string{"orders"}, func(tx *mongodb.Transaction) error {
		scoped, err := mongodb.NewTransactionRepository[OptionDocument](tx, "orders", "option_documents")
		if err != nil {
			return err
		}
		result, err := scoped.UpdateOneWithOptions(tx.Context(), bson.M{"_id": 1}, bson.M{"$inc": bson.M{"lines.$[line].score": 10}}, options.UpdateOne().SetArrayFilters([]any{bson.M{"line.score": bson.M{"$gte": 2}}}))
		if err != nil {
			return err
		}
		if result.MatchedCount != 1 {
			return errors.New("array-filter update matched no document")
		}
		if _, err := scoped.DeleteOneWithOptions(ctx, bson.M{"_id": 1}); !errors.Is(err, mongodb.ErrTransactionContext) {
			return errors.New("options API accepted wrong transaction context")
		}
		return nil
	})
	if err != nil {
		return err
	}
	value, err := r.FindByIDWithOptions(ctx, 1)
	if err != nil {
		return err
	}
	if len(value.Lines) != 2 || value.Lines[0].Score != 1 || value.Lines[1].Score != 12 {
		return errors.New("array filters updated incorrect elements")
	}
	fmt.Fprintln(s.out, "  scoped array filters updated only matching elements; wrong context was rejected")
	// Unordered operations can partially succeed. Inspect BOTH result and error;
	// never blindly retry the entire batch after a duplicate-key error.
	inserted, err := r.InsertManyWithOptions(ctx, []any{OptionDocument{ID: 2}, OptionDocument{ID: 2}, OptionDocument{ID: 3}}, options.InsertMany().SetOrdered(false))
	if !mongo.IsDuplicateKeyError(err) || inserted == nil || len(inserted.InsertedIDs) != 2 {
		return errors.New("unexpected unordered insert outcome")
	}
	bulk, err := r.BulkWriteWithOptions(ctx, []mongo.WriteModel{
		mongo.NewInsertOneModel().SetDocument(OptionDocument{ID: 4}),
		mongo.NewInsertOneModel().SetDocument(OptionDocument{ID: 3}),
		mongo.NewInsertOneModel().SetDocument(OptionDocument{ID: 5}),
	}, options.BulkWrite().SetOrdered(false))
	if !mongo.IsDuplicateKeyError(err) || bulk == nil || bulk.InsertedCount != 2 {
		return errors.New("unexpected unordered bulk outcome")
	}
	count, err := r.CountWithOptions(ctx, bson.M{}, options.Count().SetHint("_id_"))
	if err != nil {
		return err
	}
	if count != 5 {
		return errors.New("unordered writes stopped before independent operations completed")
	}
	upserted, err := r.UpsertOneWithOptions(ctx, bson.M{"_id": 6}, bson.M{"$set": bson.M{"lines": bson.A{}}}, options.UpdateOne().SetUpsert(false))
	if err != nil {
		return err
	}
	if upserted.UpsertedCount != 1 {
		return errors.New("upsert API did not enforce upsert=true")
	}
	if exists, err := r.ExistsWithOptions(ctx, bson.M{"_id": 6}, options.FindOne().SetHint("_id_")); err != nil || !exists {
		return errors.Join(errors.New("upsert missing"), err)
	}
	fmt.Fprintln(s.out, "  unordered insert/bulk: duplicate errors retained and successful writes counted; upsert flag enforced")
	return nil
}

package mongodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	database "github.com/thanhbvha/go-common/v2/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func validTenant(tenant *string) bool { return tenant == nil || messaging.ValidIdentity(*tenant, true) }
func validLimit(limit int) bool       { return limit >= 1 && limit <= 1000 }
func (s *Store) filter(tenant *string) bson.M {
	filter := bson.M{"namespace": s.cfg.Namespace}
	if tenant != nil {
		filter["tenant_id"] = *tenant
	}
	return filter
}
func (s *Store) Get(ctx context.Context, tenant, id string) (outbox.Record, error) {
	if !messaging.ValidIdentity(tenant, true) || !messaging.ValidIdentity(id, false) {
		return outbox.Record{}, messaging.ErrInvalidMessage
	}
	ctx, cancel, err := s.operation(ctx)
	if err != nil {
		return outbox.Record{}, err
	}
	defer cancel()
	var doc document
	err = s.outbox.FindOne(ctx, bson.M{"namespace": s.cfg.Namespace, "_id": identity(s.cfg.Namespace, tenant, id)}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return outbox.Record{}, messaging.ErrNotFound
	}
	if err != nil {
		return outbox.Record{}, safe(err)
	}
	return doc.record()
}

type cursor struct {
	V                           int
	Database, Prefix, Namespace string
	Tenant                      *string
	State                       outbox.State
	Position                    string
}

func (s *Store) List(ctx context.Context, req outbox.ListRequest) (outbox.Page, error) {
	var page outbox.Page
	if !validTenant(req.TenantID) || !validLimit(req.Limit) || (req.State != "" && req.State != outbox.Pending && req.State != outbox.Processing && req.State != outbox.Published && req.State != outbox.Dead) {
		return page, messaging.ErrInvalidConfig
	}
	filter := s.filter(req.TenantID)
	if req.State != "" {
		filter["state"] = req.State
	}
	if req.Cursor != "" {
		if len(req.Cursor) > 2048 {
			return page, messaging.ErrInvalidConfig
		}
		raw, err := base64.RawURLEncoding.DecodeString(req.Cursor)
		if err != nil {
			return page, messaging.ErrInvalidConfig
		}
		var c cursor
		if json.Unmarshal(raw, &c) != nil || c.V != 1 || c.Database != s.db.Name() || c.Prefix != s.cfg.Prefix || c.Namespace != s.cfg.Namespace || c.State != req.State || (c.Tenant == nil) != (req.TenantID == nil) || (c.Tenant != nil && *c.Tenant != *req.TenantID) {
			return page, messaging.ErrInvalidConfig
		}
		position, err := bson.ObjectIDFromHex(c.Position)
		if err != nil {
			return page, messaging.ErrInvalidConfig
		}
		filter["position"] = bson.M{"$gt": position}
	}
	ctx, cancel, err := s.operation(ctx)
	if err != nil {
		return page, err
	}
	defer cancel()
	rows, err := s.outbox.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "position", Value: 1}}).SetLimit(int64(req.Limit+1)))
	if err != nil {
		return page, safe(err)
	}
	var docs []document
	if err := rows.All(ctx, &docs); err != nil {
		return page, safe(err)
	}
	hasNext := len(docs) > req.Limit
	if hasNext {
		docs = docs[:req.Limit]
	}
	for _, doc := range docs {
		record, err := doc.record()
		if err != nil {
			return outbox.Page{}, err
		}
		page.Records = append(page.Records, record)
	}
	if hasNext {
		b, _ := json.Marshal(cursor{1, s.db.Name(), s.cfg.Prefix, s.cfg.Namespace, req.TenantID, req.State, docs[len(docs)-1].Position.Hex()})
		page.NextCursor = base64.RawURLEncoding.EncodeToString(b)
	}
	return page, nil
}
func (s *Store) Replay(ctx context.Context, tenant string, ids []string) (count int64, err error) {
	if !messaging.ValidIdentity(tenant, true) || !validLimit(len(ids)) {
		return 0, messaging.ErrInvalidConfig
	}
	for _, id := range ids {
		if !messaging.ValidIdentity(id, false) {
			return 0, messaging.ErrInvalidMessage
		}
	}
	ctx, cancel, err := s.operation(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()
	err = s.transaction(ctx, func(tx *database.Transaction, db *mongo.Database) error {
		count = 0
		filter := bson.M{"namespace": s.cfg.Namespace, "tenant_id": tenant, "state": "dead", "message_id": bson.M{"$in": ids}}
		result, e := db.Collection(s.outbox.Name()).UpdateMany(tx.Context(), filter, mongo.Pipeline{bson.D{{Key: "$set", Value: bson.M{"state": "pending", "attempts": 0, "available_at": "$$NOW", "failed_at": nil, "error_code": "", "replay_count": bson.M{"$add": bson.A{"$replay_count", 1}}, "replayed_at": "$$NOW"}}}})
		if e != nil {
			return e
		}
		count = result.ModifiedCount
		return nil
	})
	if err != nil {
		return 0, safe(err)
	}
	return count, nil
}
func deleteBatch(ctx context.Context, collection *mongo.Collection, filter bson.M, sort bson.D, limit int) (int64, error) {
	rows, err := collection.Find(ctx, filter, options.Find().SetSort(sort).SetLimit(int64(limit)).SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return 0, err
	}
	var docs []struct {
		ID string `bson:"_id"`
	}
	if err := rows.All(ctx, &docs); err != nil {
		return 0, err
	}
	if len(docs) == 0 {
		return 0, nil
	}
	ids := make([]string, len(docs))
	for i, doc := range docs {
		ids[i] = doc.ID
	}
	filter["_id"] = bson.M{"$in": ids}
	result, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, err
	}
	return result.DeletedCount, nil
}
func (s *Store) Cleanup(ctx context.Context, req outbox.CleanupRequest) (result outbox.CleanupResult, err error) {
	if !validTenant(req.TenantID) || !validLimit(req.Limit) || (req.PublishedBefore.IsZero() && req.InboxBefore.IsZero()) || (!req.PublishedBefore.IsZero() && req.PublishedBefore.After(time.Now())) || (!req.InboxBefore.IsZero() && req.InboxBefore.After(time.Now())) {
		return result, messaging.ErrInvalidConfig
	}
	ctx, cancel, err := s.operation(ctx)
	if err != nil {
		return result, err
	}
	defer cancel()
	err = s.transaction(ctx, func(tx *database.Transaction, db *mongo.Database) error {
		result = outbox.CleanupResult{}
		if !req.PublishedBefore.IsZero() {
			filter := s.filter(req.TenantID)
			filter["state"] = "published"
			filter["published_at"] = bson.M{"$lt": req.PublishedBefore}
			n, e := deleteBatch(tx.Context(), db.Collection(s.outbox.Name()), filter, bson.D{{Key: "published_at", Value: 1}, {Key: "_id", Value: 1}}, req.Limit)
			if e != nil {
				return e
			}
			result.Published = n
		}
		if !req.InboxBefore.IsZero() {
			filter := s.filter(req.TenantID)
			filter["processed_at"] = bson.M{"$lt": req.InboxBefore}
			n, e := deleteBatch(tx.Context(), db.Collection(s.inbox.Name()), filter, bson.D{{Key: "processed_at", Value: 1}, {Key: "_id", Value: 1}}, req.Limit)
			if e != nil {
				return e
			}
			result.Inbox = n
		}
		return nil
	})
	if err != nil {
		return outbox.CleanupResult{}, safe(err)
	}
	return result, nil
}
func (s *Store) Stats(ctx context.Context, tenant *string) (stats messaging.Stats, err error) {
	if !validTenant(tenant) {
		return stats, messaging.ErrInvalidConfig
	}
	ctx, cancel, err := s.operation(ctx)
	if err != nil {
		return stats, err
	}
	defer cancel()
	count := func(state string) bson.M {
		return bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$eq": bson.A{"$state", state}}, 1, 0}}}
	}
	rows, err := s.outbox.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$match", Value: s.filter(tenant)}},
		bson.D{{Key: "$group", Value: bson.M{"_id": nil, "pending": count("pending"), "in_flight": count("processing"), "dead": count("dead"), "oldest": bson.M{"$min": bson.M{"$cond": bson.A{bson.M{"$eq": bson.A{"$state", "pending"}}, "$created_at", nil}}}}}},
		bson.D{{Key: "$project", Value: bson.M{"pending": 1, "in_flight": 1, "dead": 1, "age": bson.M{"$max": bson.A{0, bson.M{"$ifNull": bson.A{bson.M{"$subtract": bson.A{"$$NOW", "$oldest"}}, 0}}}}}}},
	})
	if err != nil {
		return stats, safe(err)
	}
	var docs []struct {
		Pending  int64 `bson:"pending"`
		InFlight int64 `bson:"in_flight"`
		Dead     int64 `bson:"dead"`
		Age      int64 `bson:"age"`
	}
	if err := rows.All(ctx, &docs); err != nil {
		return stats, safe(err)
	}
	if len(docs) > 0 {
		r := docs[0]
		stats = messaging.Stats{Pending: r.Pending, InFlight: r.InFlight, Dead: r.Dead, OldestPendingAge: time.Duration(r.Age) * time.Millisecond}
	}
	messaging.Observe(ctx, s.cfg.Observer, messaging.Observation{Operation: "outbox_stats", Outcome: "success", Stats: &stats})
	return stats, nil
}

package mongodb

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func (s *Store) Claim(ctx context.Context, req outbox.ClaimRequest) ([]outbox.Claim, error) {
	if req.Limit < 1 || req.Limit > 1000 || req.MaxAttempts < 1 || req.MaxAttempts > 1000000 || req.Lease < time.Millisecond || req.Lease > 24*time.Hour {
		return nil, messaging.ErrInvalidConfig
	}
	ctx, cancel, err := s.operation(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	expired := bson.M{"state": "processing", "$expr": bson.M{"$lte": bson.A{"$lease_until", "$$NOW"}}}
	// Bounded exhaustion recovery, including a crash on the final attempt.
	filter := bson.M{"namespace": s.cfg.Namespace, "attempts": bson.M{"$gte": req.MaxAttempts}, "$or": bson.A{bson.M{"state": "pending"}, expired}}
	rows, err := s.outbox.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "position", Value: 1}}).SetLimit(int64(req.Limit)).SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, safe(err)
	}
	var exhausted []struct {
		ID string `bson:"_id"`
	}
	if err := rows.All(ctx, &exhausted); err != nil {
		return nil, safe(err)
	}
	if len(exhausted) > 0 {
		ids := make([]string, len(exhausted))
		for i, doc := range exhausted {
			ids[i] = doc.ID
		}
		filter["_id"] = bson.M{"$in": ids}
		// Recheck state/expiry so recovery cannot quarantine a newly owned lease.
		if _, err := s.outbox.UpdateMany(ctx, filter, mongo.Pipeline{bson.D{{Key: "$set", Value: bson.M{"state": "dead", "failed_at": "$$NOW", "error_code": "attempts_exhausted", "claim_token": nil, "lease_until": nil}}}}); err != nil {
			return nil, safe(err)
		}
	}
	claims := make([]outbox.Claim, 0, req.Limit)
	for i := 0; i < req.Limit; i++ {
		filter := bson.M{"namespace": s.cfg.Namespace, "attempts": bson.M{"$lt": req.MaxAttempts}, "$or": bson.A{bson.M{"state": "pending", "$expr": bson.M{"$lte": bson.A{"$available_at", "$$NOW"}}}, expired}}
		update := mongo.Pipeline{bson.D{{Key: "$set", Value: bson.M{"state": "processing", "attempts": bson.M{"$add": bson.A{"$attempts", 1}}, "claim_token": uuid.NewString(), "lease_until": bson.M{"$add": bson.A{"$$NOW", req.Lease.Milliseconds()}}}}}}
		var doc document
		err := s.outbox.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetSort(bson.D{{Key: "available_at", Value: 1}, {Key: "position", Value: 1}}).SetReturnDocument(options.After)).Decode(&doc)
		if errors.Is(err, mongo.ErrNoDocuments) {
			break
		}
		if err != nil {
			return nil, safe(err)
		}
		message, err := doc.message()
		if err != nil {
			return nil, err
		}
		if doc.ClaimToken == nil || doc.LeaseUntil == nil {
			return nil, messaging.ErrSchema
		}
		claims = append(claims, outbox.Claim{Message: message, Token: *doc.ClaimToken, Attempt: doc.Attempts, LeaseUntil: *doc.LeaseUntil})
	}
	// Each document is independently majority-acknowledged. On partial/ambiguous
	// failure return no claims; earlier owned documents recover after lease expiry.
	return claims, nil
}
func (s *Store) settle(ctx context.Context, c outbox.Claim, fields bson.M) error {
	if c.Message.Namespace != s.cfg.Namespace || !messaging.ValidIdentity(c.Message.ID, false) || !messaging.ValidIdentity(c.Message.TenantID, true) || c.Token == "" {
		return messaging.ErrInvalidMessage
	}
	ctx, cancel, err := s.operation(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	filter := bson.M{"_id": identity(s.cfg.Namespace, c.Message.TenantID, c.Message.ID), "namespace": s.cfg.Namespace, "state": "processing", "claim_token": c.Token, "$expr": bson.M{"$gt": bson.A{"$lease_until", "$$NOW"}}}
	fields["claim_token"] = nil
	fields["lease_until"] = nil
	result, err := s.outbox.UpdateOne(ctx, filter, mongo.Pipeline{bson.D{{Key: "$set", Value: fields}}})
	if err != nil {
		return safe(err)
	}
	if result.MatchedCount != 1 {
		return messaging.ErrLeaseLost
	}
	return nil
}
func (s *Store) MarkPublished(ctx context.Context, c outbox.Claim) error {
	return s.settle(ctx, c, bson.M{"state": "published", "published_at": "$$NOW", "error_code": ""})
}
func validCode(code string) bool {
	if code == "" || len(code) > 64 {
		return false
	}
	for _, r := range code {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') {
			return false
		}
	}
	return true
}
func (s *Store) Retry(ctx context.Context, c outbox.Claim, delay time.Duration, code string) error {
	if delay < 0 || delay > 24*time.Hour || !validCode(code) {
		return messaging.ErrInvalidConfig
	}
	return s.settle(ctx, c, bson.M{"state": "pending", "available_at": bson.M{"$add": bson.A{"$$NOW", delay.Milliseconds()}}, "error_code": code})
}
func (s *Store) Quarantine(ctx context.Context, c outbox.Claim, code string) error {
	if !validCode(code) {
		return messaging.ErrInvalidConfig
	}
	return s.settle(ctx, c, bson.M{"state": "dead", "failed_at": "$$NOW", "error_code": code})
}

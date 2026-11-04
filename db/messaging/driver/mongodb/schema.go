package mongodb

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	database "github.com/thanhbvha/go-common/v2/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type schemaDefinition struct {
	suffix    string
	validator bson.M
	indexes   []mongo.IndexModel
}

func definitions() []schemaDefinition {
	field := func(kind any) bson.M { return bson.M{"bsonType": kind} }
	properties := bson.M{}
	for _, name := range []string{"_id", "namespace", "tenant_id", "message_id", "kind", "type", "ordering_key", "concurrency_key", "fingerprint", "immutable_hash", "state", "error_code"} {
		properties[name] = field("string")
	}
	for _, name := range []string{"attempts", "schema_version", "replay_count"} {
		properties[name] = bson.M{"bsonType": bson.A{"int", "long"}, "minimum": 0}
	}
	properties["schema_version"] = bson.M{"bsonType": bson.A{"int", "long"}, "minimum": 1}
	properties["position"] = field("objectId")
	properties["payload"] = field("binData")
	properties["headers"] = field("binData")
	for _, name := range []string{"created_at", "available_at"} {
		properties[name] = field("date")
	}
	for _, name := range []string{"scheduled_at", "lease_until", "published_at", "failed_at", "replayed_at"} {
		properties[name] = field(bson.A{"date", "null"})
	}
	properties["claim_token"] = field(bson.A{"string", "null"})
	properties["kind"] = bson.M{"enum": bson.A{"task", "event"}}
	properties["state"] = bson.M{"enum": bson.A{"pending", "processing", "published", "dead"}}
	required := bson.A{"_id", "namespace", "tenant_id", "message_id", "position", "kind", "type", "schema_version", "payload", "headers", "fingerprint", "immutable_hash", "state", "attempts", "available_at", "created_at", "claim_token", "lease_until", "replay_count", "error_code"}
	outboxValidator := bson.M{"$and": bson.A{
		bson.M{"$jsonSchema": bson.M{"bsonType": "object", "required": required, "properties": properties}},
		bson.M{"$or": bson.A{bson.M{"state": "processing", "claim_token": bson.M{"$type": "string"}, "lease_until": bson.M{"$type": "date"}}, bson.M{"state": bson.M{"$ne": "processing"}, "claim_token": nil, "lease_until": nil}}},
	}}
	index := func(name string, keys bson.D, unique bool) mongo.IndexModel {
		return mongo.IndexModel{Keys: keys, Options: options.Index().SetName(name).SetUnique(unique)}
	}
	return []schemaDefinition{
		{"outbox", outboxValidator, []mongo.IndexModel{
			index("identity", bson.D{{Key: "namespace", Value: 1}, {Key: "tenant_id", Value: 1}, {Key: "message_id", Value: 1}}, true),
			index("position", bson.D{{Key: "position", Value: 1}}, true),
			index("ready", bson.D{{Key: "namespace", Value: 1}, {Key: "state", Value: 1}, {Key: "available_at", Value: 1}, {Key: "position", Value: 1}}, false),
			index("expired", bson.D{{Key: "namespace", Value: 1}, {Key: "state", Value: 1}, {Key: "lease_until", Value: 1}, {Key: "position", Value: 1}}, false),
			index("published_cleanup", bson.D{{Key: "namespace", Value: 1}, {Key: "state", Value: 1}, {Key: "published_at", Value: 1}}, false),
		}},
		{"inbox", bson.M{"$jsonSchema": bson.M{"bsonType": "object", "required": bson.A{"_id", "namespace", "consumer", "tenant_id", "message_id", "fingerprint", "processed_at"}, "properties": bson.M{"_id": field("string"), "namespace": field("string"), "consumer": field("string"), "tenant_id": field("string"), "message_id": field("string"), "fingerprint": field("string"), "processed_at": field("date")}}}, []mongo.IndexModel{
			index("identity", bson.D{{Key: "namespace", Value: 1}, {Key: "consumer", Value: 1}, {Key: "tenant_id", Value: 1}, {Key: "message_id", Value: 1}}, true),
			index("cleanup", bson.D{{Key: "namespace", Value: 1}, {Key: "processed_at", Value: 1}}, false),
		}},
	}
}

func canonical(value any) (string, error) {
	b, err := bson.MarshalExtJSON(value, true, false)
	if err != nil {
		return "", err
	}
	var plain any
	if err := json.Unmarshal(b, &plain); err != nil {
		return "", err
	}
	return hash(plain), nil
}
func (s *Store) inspectCollections(ctx context.Context, indexes bool) error {
	for _, definition := range definitions() {
		name := s.cfg.Prefix + "_" + definition.suffix
		specs, err := s.db.ListCollectionSpecifications(ctx, bson.M{"name": name})
		if err != nil {
			return err
		}
		if len(specs) != 1 || specs[0].Type != "collection" {
			return messaging.ErrSchema
		}
		if collation, ok := specs[0].Options.Lookup("collation").DocumentOK(); ok {
			if locale, ok := collation.Lookup("locale").StringValueOK(); !ok || locale != "simple" {
				return messaging.ErrSchema
			}
		}
		validator, ok := specs[0].Options.Lookup("validator").DocumentOK()
		if !ok {
			return messaging.ErrSchema
		}
		want, err := canonical(definition.validator)
		if err != nil {
			return err
		}
		got, err := canonical(validator)
		if err != nil || want != got {
			return messaging.ErrSchema
		}
		// An otherwise matching validator must not have been disabled or weakened.
		for key, expected := range map[string]string{"validationLevel": "strict", "validationAction": "error"} {
			if value, ok := specs[0].Options.Lookup(key).StringValueOK(); ok && value != expected {
				return messaging.ErrSchema
			}
		}
		if !indexes {
			continue
		}
		cursor, err := s.db.Collection(name).Indexes().List(ctx)
		if err != nil {
			return err
		}
		var actual []struct {
			Name      string `bson:"name"`
			Key       bson.D `bson:"key"`
			Unique    bool   `bson:"unique"`
			Sparse    bool   `bson:"sparse"`
			Partial   bson.M `bson:"partialFilterExpression"`
			Collation bson.M `bson:"collation"`
			TTL       any    `bson:"expireAfterSeconds"`
		}
		err = cursor.All(ctx, &actual)
		if err != nil {
			return err
		}
		for _, index := range actual {
			if index.TTL != nil || (len(index.Collation) > 0 && index.Collation["locale"] != "simple") {
				return messaging.ErrSchema
			}
		}
		for _, required := range definition.indexes {
			var requiredOptions options.IndexOptions
			for _, apply := range required.Options.List() {
				if err := apply(&requiredOptions); err != nil {
					return err
				}
			}
			found := false
			wantKey, _ := bson.Marshal(required.Keys)
			for _, candidate := range actual {
				key, _ := bson.Marshal(candidate.Key)
				if candidate.Name == *requiredOptions.Name && string(key) == string(wantKey) && candidate.Unique == *requiredOptions.Unique && !candidate.Sparse && len(candidate.Partial) == 0 {
					found = true
					break
				}
			}
			if !found {
				return messaging.ErrSchema
			}
		}
	}
	return nil
}
func (s *Store) inspect(ctx context.Context) error {
	var marker struct {
		Version int `bson:"version"`
	}
	if err := s.schema.FindOne(ctx, bson.M{"_id": "schema"}).Decode(&marker); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return messaging.ErrSchema
		}
		return err
	}
	if marker.Version != 1 {
		return messaging.ErrSchema
	}
	return s.inspectCollections(ctx, true)
}

// Provision explicitly creates version-1 collections, strict validators and
// indexes. It never weakens or silently upgrades an existing schema. There is no
// automatic TTL: receipt retention and quarantine recovery belong to the app.
func Provision(ctx context.Context, manager *database.Manager, cfg Config) error {
	configured, err := configuration(cfg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, configured.OperationTimeout)
	defer cancel()
	if mongo.SessionFromContext(ctx) != nil {
		return database.ErrNestedTransaction
	}
	s, err := bind(ctx, manager, configured)
	if err != nil {
		return err
	}
	var marker struct {
		Version int `bson:"version"`
	}
	err = s.schema.FindOne(ctx, bson.M{"_id": "schema"}).Decode(&marker)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return safe(err)
	}
	if err == nil && marker.Version != 1 {
		return messaging.ErrSchema
	}
	for _, definition := range definitions() {
		err = s.db.CreateCollection(ctx, s.cfg.Prefix+"_"+definition.suffix, options.CreateCollection().SetValidator(definition.validator).SetValidationLevel("strict").SetValidationAction("error"))
		var server mongo.ServerError
		if err != nil && !(errors.As(err, &server) && server.HasErrorCode(48)) {
			return safe(err)
		}
	}
	if err := s.inspectCollections(ctx, false); err != nil {
		return safe(err)
	}
	for _, definition := range definitions() {
		if _, err := s.db.Collection(s.cfg.Prefix+"_"+definition.suffix).Indexes().CreateMany(ctx, definition.indexes); err != nil {
			return safe(err)
		}
	}
	_, err = s.schema.UpdateOne(ctx, bson.M{"_id": "schema"}, bson.M{"$setOnInsert": bson.M{"version": 1}}, options.UpdateOne().SetUpsert(true))
	if err != nil {
		return safe(err)
	}
	return safe(s.inspect(ctx))
}

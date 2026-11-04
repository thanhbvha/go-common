// Package mongodb implements transactional messaging on transaction-capable
// MongoDB 4.2+ deployments. It borrows a db/mongodb Manager and its native scopes.
package mongodb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	database "github.com/thanhbvha/go-common/v2/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type Config struct {
	Namespace string
	Alias     string
	// Prefix defaults to messaging. Collections are Prefix_outbox/inbox/schema.
	Prefix string
	// TransactionAliases declares additional same-client databases for Consume.
	TransactionAliases []string
	OperationTimeout   time.Duration
	Observer           messaging.Observer
}
type Store struct {
	manager               *database.Manager
	db                    *mongo.Database
	cfg                   Config
	aliases               []string
	outbox, inbox, schema *mongo.Collection
}

// Error is safe to print; original server errors remain available through
// errors.As. Do not log unwrapped errors, which can include document contents.
type Error struct{ cause error }

func (*Error) Error() string   { return "messaging mongodb: database operation failed" }
func (e *Error) Unwrap() error { return e.cause }
func safe(err error) error {
	if err == nil {
		return nil
	}
	var server mongo.ServerError
	if errors.As(err, &server) && (server.HasErrorCode(26) || server.HasErrorCode(121)) {
		err = errors.Join(messaging.ErrSchema, err)
	}
	return &Error{cause: err}
}

var prefixPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,39}$`)

func configuration(cfg Config) (Config, error) {
	if cfg.Prefix == "" {
		cfg.Prefix = "messaging"
	}
	if cfg.OperationTimeout == 0 {
		cfg.OperationTimeout = 10 * time.Second
	}
	if !messaging.ValidIdentity(cfg.Namespace, false) || !messaging.ValidIdentity(cfg.Alias, false) || !prefixPattern.MatchString(cfg.Prefix) || cfg.OperationTimeout < time.Millisecond || cfg.OperationTimeout > 24*time.Hour {
		return cfg, messaging.ErrInvalidConfig
	}
	cfg.TransactionAliases = append([]string(nil), cfg.TransactionAliases...)
	for _, alias := range cfg.TransactionAliases {
		if !messaging.ValidIdentity(alias, false) {
			return cfg, messaging.ErrInvalidConfig
		}
	}
	return cfg, nil
}
func bind(ctx context.Context, manager *database.Manager, cfg Config) (*Store, error) {
	cfg, err := configuration(cfg)
	if err != nil {
		return nil, err
	}
	if manager == nil {
		return nil, messaging.ErrInvalidConfig
	}
	db, err := manager.Lookup(cfg.Alias)
	if err != nil {
		return nil, safe(err)
	}
	caps, err := database.DetectCapabilities(ctx, db.Client())
	if err != nil {
		return nil, safe(err)
	}
	if !caps.Transactions || caps.MaxWireVersion < 8 {
		return nil, database.ErrTransactionsUnsupported
	}
	db = db.Client().Database(db.Name(), options.Database().SetReadConcern(readconcern.Majority()).SetWriteConcern(writeconcern.Majority()).SetReadPreference(readpref.Primary()))
	aliases := []string{cfg.Alias}
	seen := map[string]bool{cfg.Alias: true}
	for _, alias := range cfg.TransactionAliases {
		other, e := manager.Lookup(alias)
		if e != nil {
			return nil, safe(e)
		}
		if other.Client() != db.Client() {
			return nil, database.ErrTransactionClientMismatch
		}
		if !seen[alias] {
			aliases = append(aliases, alias)
			seen[alias] = true
		}
	}
	return &Store{manager: manager, db: db, cfg: cfg, aliases: aliases, outbox: db.Collection(cfg.Prefix + "_outbox"), inbox: db.Collection(cfg.Prefix + "_inbox"), schema: db.Collection(cfg.Prefix + "_schema")}, nil
}

// New performs read-only validation. Run Provision in a deployment job first.
func New(ctx context.Context, manager *database.Manager, cfg Config) (*Store, error) {
	configured, err := configuration(cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, configured.OperationTimeout)
	defer cancel()
	if mongo.SessionFromContext(ctx) != nil {
		return nil, database.ErrNestedTransaction
	}
	s, err := bind(ctx, manager, configured)
	if err != nil {
		return nil, err
	}
	if err := s.inspect(ctx); err != nil {
		return nil, safe(err)
	}
	return s, nil
}
func (s *Store) operation(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if mongo.SessionFromContext(ctx) != nil {
		return nil, nil, database.ErrNestedTransaction
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	db, err := s.manager.Lookup(s.cfg.Alias)
	if err != nil {
		return nil, nil, safe(err)
	}
	if db.Client() != s.db.Client() || db.Name() != s.db.Name() {
		return nil, nil, database.ErrTransactionClientMismatch
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.OperationTimeout)
	return ctx, cancel, nil
}
func (s *Store) transactionDatabase(tx *database.Transaction) (*mongo.Database, error) {
	if tx == nil {
		return nil, database.ErrTransactionClosed
	}
	db, err := tx.Database(s.cfg.Alias)
	if err != nil {
		return nil, err
	}
	if db.Client() != s.db.Client() || db.Name() != s.db.Name() {
		return nil, database.ErrTransactionClientMismatch
	}
	return db, nil
}
func (s *Store) transaction(ctx context.Context, fn func(*database.Transaction, *mongo.Database) error) error {
	err := s.manager.WithTransactionAcross(ctx, s.aliases, func(tx *database.Transaction) error {
		db, err := s.transactionDatabase(tx)
		if err != nil {
			return err
		}
		return fn(tx, db)
	}, TransactionOptions())
	// The native convenient API may finish commit after the caller's deadline.
	// Never expose a successful handler result after cancellation; a redelivery
	// can reconcile the committed receipt without repeating business effects.
	if err == nil {
		return ctx.Err()
	}
	return err
}

// TransactionOptions returns fresh durable defaults for producer callbacks that
// call Append. Consume/admin transactions use these same options internally.
func TransactionOptions() *options.TransactionOptionsBuilder {
	return options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()).SetReadPreference(readpref.Primary())
}

func identity(namespace, tenant, id string) string { return hash([]string{namespace, tenant, id}) }
func receiptID(r inbox.Receipt) string {
	return hash([]string{r.Namespace, r.Consumer, r.TenantID, r.ID})
}
func hash(value any) string {
	b, _ := json.Marshal(value)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func literal(value any) bson.M { return bson.M{"$literal": value} }

type document struct {
	ID             string         `bson:"_id"`
	Position       bson.ObjectID  `bson:"position"`
	Namespace      string         `bson:"namespace"`
	TenantID       string         `bson:"tenant_id"`
	MessageID      string         `bson:"message_id"`
	Kind           messaging.Kind `bson:"kind"`
	Type           string         `bson:"type"`
	SchemaVersion  int            `bson:"schema_version"`
	Payload        []byte         `bson:"payload"`
	Headers        []byte         `bson:"headers"`
	OrderingKey    string         `bson:"ordering_key"`
	ConcurrencyKey string         `bson:"concurrency_key"`
	Fingerprint    string         `bson:"fingerprint"`
	ImmutableHash  string         `bson:"immutable_hash"`
	ScheduledAt    *time.Time     `bson:"scheduled_at"`
	AvailableAt    time.Time      `bson:"available_at"`
	CreatedAt      time.Time      `bson:"created_at"`
	State          outbox.State   `bson:"state"`
	Attempts       int            `bson:"attempts"`
	ClaimToken     *string        `bson:"claim_token"`
	LeaseUntil     *time.Time     `bson:"lease_until"`
	PublishedAt    *time.Time     `bson:"published_at"`
	FailedAt       *time.Time     `bson:"failed_at"`
	ErrorCode      string         `bson:"error_code"`
	ReplayCount    int            `bson:"replay_count"`
	ReplayedAt     *time.Time     `bson:"replayed_at"`
}

func (d document) message() (messaging.Message, error) {
	m := messaging.Message{ID: d.MessageID, Namespace: d.Namespace, TenantID: d.TenantID, Kind: d.Kind, Type: d.Type, SchemaVersion: d.SchemaVersion, Payload: d.Payload, OrderingKey: d.OrderingKey, ConcurrencyKey: d.ConcurrencyKey}
	if err := json.Unmarshal(d.Headers, &m.Headers); err != nil {
		return m, messaging.ErrInvalidMessage
	}
	if d.ScheduledAt != nil {
		m.AvailableAt = *d.ScheduledAt
	}
	return m, m.Validate()
}
func (d document) record() (outbox.Record, error) {
	m, err := d.message()
	return outbox.Record{Message: m, State: d.State, Attempts: d.Attempts, CreatedAt: d.CreatedAt, PublishedAt: d.PublishedAt, FailedAt: d.FailedAt, ErrorCode: d.ErrorCode, ReplayCount: d.ReplayCount, ReplayedAt: d.ReplayedAt}, err
}

// Append joins the caller's native transaction. WithTransactionAcross may repeat
// that callback; generate IDs outside it and propagate every database error.
func (s *Store) Append(tx *database.Transaction, m messaging.Message) error {
	db, err := s.transactionDatabase(tx)
	if err != nil {
		return err
	}
	if m.Namespace != s.cfg.Namespace {
		return messaging.ErrInvalidMessage
	}
	fingerprint, err := m.Fingerprint()
	if err != nil {
		return err
	}
	headers, err := json.Marshal(m.Headers)
	if err != nil {
		return messaging.ErrInvalidMessage
	}
	var scheduled *time.Time
	if !m.AvailableAt.IsZero() {
		v := m.AvailableAt.UTC().Truncate(time.Millisecond)
		scheduled = &v
	}
	doc := document{ID: identity(m.Namespace, m.TenantID, m.ID), Position: bson.NewObjectID(), Namespace: m.Namespace, TenantID: m.TenantID, MessageID: m.ID, Kind: m.Kind, Type: m.Type, SchemaVersion: m.SchemaVersion,
		Payload: []byte(m.Payload), Headers: headers, OrderingKey: m.OrderingKey, ConcurrencyKey: m.ConcurrencyKey, Fingerprint: fingerprint, ImmutableHash: hash([]any{fingerprint, m.Headers, m.OrderingKey, m.ConcurrencyKey, scheduled}), ScheduledAt: scheduled, State: outbox.Pending}
	var available any = "$$NOW"
	if scheduled != nil {
		available = *scheduled
	}
	// Literal protects payload/header strings beginning with '$'. Existing rows
	// remain untouched; the returned immutable hash resolves a valid replay.
	update := mongo.Pipeline{bson.D{{Key: "$replaceWith", Value: bson.M{"$cond": bson.A{
		bson.M{"$eq": bson.A{bson.M{"$type": "$immutable_hash"}, "missing"}},
		bson.M{"$mergeObjects": bson.A{literal(doc), bson.M{"created_at": "$$NOW", "available_at": available}}}, "$$ROOT",
	}}}}}
	var stored document
	err = db.Collection(s.outbox.Name()).FindOneAndUpdate(tx.Context(), bson.M{"_id": doc.ID}, update, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&stored)
	if err != nil {
		return safe(err)
	}
	if stored.ImmutableHash != doc.ImmutableHash {
		return messaging.ErrIdentityConflict
	}
	return nil
}

type receiptDocument struct {
	ID          string    `bson:"_id"`
	Namespace   string    `bson:"namespace"`
	Consumer    string    `bson:"consumer"`
	TenantID    string    `bson:"tenant_id"`
	MessageID   string    `bson:"message_id"`
	Fingerprint string    `bson:"fingerprint"`
	ProcessedAt time.Time `bson:"processed_at"`
}
type receiptRace struct{ cause error }

func (*receiptRace) Error() string   { return "messaging mongodb: concurrent receipt" }
func (e *receiptRace) Unwrap() error { return e.cause }

func (s *Store) Consume(ctx context.Context, r inbox.Receipt, handler func(*database.Transaction) error) (result inbox.Result, err error) {
	if r.Namespace != s.cfg.Namespace || handler == nil {
		return "", messaging.ErrInvalidMessage
	}
	if err := r.Validate(); err != nil {
		return "", err
	}
	started := time.Now()
	defer func() {
		outcome := string(result)
		if err != nil {
			outcome = "error"
		}
		if errors.Is(err, messaging.ErrIdentityConflict) {
			outcome = "conflict"
		}
		messaging.Observe(ctx, s.cfg.Observer, messaging.Observation{Operation: "inbox", Outcome: outcome, Duration: time.Since(started)})
	}()
	ctx, cancel, err := s.operation(ctx)
	if err != nil {
		return "", err
	}
	defer cancel()
	for {
		result = ""
		err = s.transaction(ctx, func(tx *database.Transaction, db *mongo.Database) error {
			result = "" // Native transaction retries must not leak an earlier attempt.
			collection := db.Collection(s.inbox.Name())
			var existing receiptDocument
			e := collection.FindOne(tx.Context(), bson.M{"_id": receiptID(r)}).Decode(&existing)
			if e == nil {
				if existing.Fingerprint != r.Fingerprint {
					return messaging.ErrIdentityConflict
				}
				result = inbox.Duplicate
				return nil
			}
			if !errors.Is(e, mongo.ErrNoDocuments) {
				return e
			}
			doc := receiptDocument{ID: receiptID(r), Namespace: r.Namespace, Consumer: r.Consumer, TenantID: r.TenantID, MessageID: r.ID, Fingerprint: r.Fingerprint}
			// Avoid catching duplicate-key errors from the application's callback.
			// An upsert race aborts the transaction; it cannot be read in that scope.
			inserted, e := collection.UpdateOne(tx.Context(), bson.M{"_id": doc.ID}, mongo.Pipeline{bson.D{{Key: "$replaceWith", Value: bson.M{"$mergeObjects": bson.A{literal(doc), bson.M{"processed_at": "$$NOW"}}}}}}, options.UpdateOne().SetUpsert(true))
			if mongo.IsDuplicateKeyError(e) {
				return &receiptRace{cause: e}
			}
			if e != nil {
				return e
			}
			if inserted.UpsertedCount != 1 {
				return &receiptRace{cause: messaging.ErrIdentityConflict}
			}
			if e := handler(tx); e != nil {
				return e
			}
			result = inbox.Applied
			return nil
		})
		if err == nil {
			return result, nil
		}
		var race *receiptRace
		var server mongo.ServerError
		if !errors.As(err, &race) || (errors.As(err, &server) && server.HasErrorLabel("UnknownTransactionCommitResult")) {
			return "", safe(err)
		}
		// Only a receipt insertion race retries outside the native callback. An
		// unknown commit or application unique violation must remain a failure.
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", safe(ctx.Err())
		case <-timer.C:
		}
	}
}

var _ outbox.Store = (*Store)(nil)
var _ outbox.Admin = (*Store)(nil)
var _ inbox.Processor[*database.Transaction] = (*Store)(nil)

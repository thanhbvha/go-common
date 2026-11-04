// Package postgres implements transactional messaging on PostgreSQL 16–18 using
// the ORM client. The caller owns the pool and deployment migration lifecycle.
package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	"github.com/thanhbvha/go-common/v2/db/orm"
	"gorm.io/gorm"
)

//go:embed migrations/*.sql
var migrations embed.FS
var schemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// MigrationFS returns versioned SQL for the chosen schema, consumable by
// db/migration. The caller runs Up in one deployment job, never service startup.
// Goose version 202611040001 must be included in the application's migration
// history or managed with a separate migration metadata table/pool search_path.
func MigrationFS(schema string) (fs.FS, error) {
	if !schemaPattern.MatchString(schema) {
		return nil, messaging.ErrInvalidConfig
	}
	b, err := migrations.ReadFile("migrations/202611040001_messaging.sql")
	if err != nil {
		return nil, err
	}
	return migrationFS{data: []byte(strings.ReplaceAll(string(b), "{{schema}}", `"`+schema+`"`))}, nil
}

type migrationFS struct{ data []byte }

func (m migrationFS) Open(name string) (fs.File, error) {
	if name == "." {
		f, err := migrations.Open("migrations")
		if err != nil {
			return nil, err
		}
		return &migrationDir{ReadDirFile: f.(fs.ReadDirFile), size: int64(len(m.data))}, nil
	}
	if name != "202611040001_messaging.sql" {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	f, err := migrations.Open("migrations/" + name)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	_ = f.Close()
	if err != nil {
		return nil, err
	}
	return &migrationFile{Reader: bytes.NewReader(m.data), info: migrationInfo{info, int64(len(m.data))}}, nil
}

type migrationInfo struct {
	fs.FileInfo
	size int64
}

func (m migrationInfo) Size() int64 { return m.size }

type migrationDir struct {
	fs.ReadDirFile
	size int64
}

func (d *migrationDir) ReadDir(n int) ([]fs.DirEntry, error) {
	entries, err := d.ReadDirFile.ReadDir(n)
	for i, entry := range entries {
		info, e := entry.Info()
		if e != nil {
			return nil, e
		}
		entries[i] = fs.FileInfoToDirEntry(migrationInfo{info, d.size})
	}
	return entries, err
}

type migrationFile struct {
	*bytes.Reader
	info fs.FileInfo
}

func (m *migrationFile) Stat() (fs.FileInfo, error) { return m.info, nil }
func (*migrationFile) Close() error                 { return nil }

type Config struct {
	Namespace, Schema string
	Observer          messaging.Observer
}
type Store struct {
	client                 *orm.Client
	cfg                    Config
	outbox, inbox, version string
}

// Error has a safe printable form and preserves the original error chain for
// classification. Never log unwrapped database errors.
type Error struct{ cause error }

func (*Error) Error() string   { return "messaging postgres: database operation failed" }
func (e *Error) Unwrap() error { return e.cause }
func safe(err error) error {
	if err == nil {
		return nil
	}
	var state interface{ SQLState() string }
	if errors.As(err, &state) && (state.SQLState() == "42P01" || state.SQLState() == "42703") {
		err = errors.Join(messaging.ErrSchema, err)
	}
	return &Error{cause: err}
}

func New(ctx context.Context, client *orm.Client, cfg Config) (*Store, error) {
	if client == nil || client.Capabilities().Name != "postgres" || !messaging.ValidIdentity(cfg.Namespace, false) || !schemaPattern.MatchString(cfg.Schema) {
		return nil, messaging.ErrInvalidConfig
	}
	prefix := `"` + cfg.Schema + `".`
	s := &Store{client: client, cfg: cfg, outbox: prefix + "outbox", inbox: prefix + "inbox", version: prefix + "messaging_schema"}
	err := client.WithTransaction(ctx, &sql.TxOptions{ReadOnly: true}, func(tx *orm.Transaction) error {
		db, err := tx.DB()
		if err != nil {
			return err
		}
		var versions []int
		if err := db.Raw("SELECT version FROM " + s.version + " WHERE singleton = true").Scan(&versions).Error; err != nil {
			return messaging.ErrSchema
		}
		if len(versions) != 1 || versions[0] != 1 {
			return messaging.ErrSchema
		}
		// Verify essential columns and permissions without modifying data.
		if err := db.Exec("SELECT seq, immutable_hash, claim_token, lease_until, replay_count, scheduled_at FROM " + s.outbox + " LIMIT 0").Error; err != nil {
			return messaging.ErrSchema
		}
		if err := db.Exec("SELECT namespace, consumer, tenant_id, message_id, fingerprint, processed_at FROM " + s.inbox + " LIMIT 0").Error; err != nil {
			return messaging.ErrSchema
		}
		return nil
	})
	if err != nil {
		return nil, safe(err)
	}
	return s, nil
}

func (s *Store) transaction(ctx context.Context, fn func(*gorm.DB) error) error {
	return safe(s.client.WithTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}, func(tx *orm.Transaction) error {
		db, err := tx.DB()
		if err != nil {
			return err
		}
		return fn(db)
	}))
}
func (s *Store) Append(tx *orm.Transaction, m messaging.Message) error {
	if err := tx.ValidateClient(s.client); err != nil {
		return err
	}
	if m.Namespace != s.cfg.Namespace {
		return messaging.ErrInvalidMessage
	}
	fingerprint, err := m.Fingerprint()
	if err != nil {
		return err
	}
	// Compare ALL immutable publication fields on producer replay, including
	// routing, scheduling and application headers (inbox hash excludes these).
	headerBytes, err := json.Marshal(m.Headers)
	if err != nil {
		return messaging.ErrInvalidMessage
	}
	var scheduled *time.Time
	if !m.AvailableAt.IsZero() {
		v := m.AvailableAt.UTC().Truncate(time.Microsecond)
		scheduled = &v
	}
	immutable, err := json.Marshal([]any{fingerprint, m.Headers, m.OrderingKey, m.ConcurrencyKey, scheduled})
	if err != nil {
		return messaging.ErrInvalidMessage
	}
	h := sha256.Sum256(immutable)
	immutableHash := hex.EncodeToString(h[:])
	db, err := tx.DB()
	if err != nil {
		return err
	}
	r := db.Exec("INSERT INTO "+s.outbox+` (namespace,tenant_id,message_id,kind,type,schema_version,payload,headers,ordering_key,concurrency_key,fingerprint,immutable_hash,scheduled_at,available_at)
 VALUES (?,?,?,?,?,?,?,?::jsonb,?,?,?,?,?,COALESCE(?,clock_timestamp()))
 ON CONFLICT (namespace,tenant_id,message_id) DO NOTHING`, m.Namespace, m.TenantID, m.ID, m.Kind, m.Type, m.SchemaVersion, []byte(m.Payload), string(headerBytes), m.OrderingKey, m.ConcurrencyKey, fingerprint, immutableHash, scheduled, scheduled)
	if r.Error != nil {
		return safe(r.Error)
	}
	if r.RowsAffected == 1 {
		return nil
	}
	var rows []struct{ ImmutableHash string }
	if err := db.Raw("SELECT immutable_hash FROM "+s.outbox+" WHERE namespace=? AND tenant_id=? AND message_id=?", m.Namespace, m.TenantID, m.ID).Scan(&rows).Error; err != nil {
		return safe(err)
	}
	if len(rows) != 1 || rows[0].ImmutableHash != immutableHash {
		return messaging.ErrIdentityConflict
	}
	return nil
}

func (s *Store) Consume(ctx context.Context, receipt inbox.Receipt, handler func(*orm.Transaction) error) (result inbox.Result, err error) {
	if receipt.Namespace != s.cfg.Namespace || handler == nil {
		return "", messaging.ErrInvalidMessage
	}
	if err := receipt.Validate(); err != nil {
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
	err = s.client.WithTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}, func(tx *orm.Transaction) error {
		db, e := tx.DB()
		if e != nil {
			return e
		}
		r := db.Exec("INSERT INTO "+s.inbox+` (namespace,consumer,tenant_id,message_id,fingerprint) VALUES (?,?,?,?,?)
 ON CONFLICT (namespace,consumer,tenant_id,message_id) DO NOTHING`, receipt.Namespace, receipt.Consumer, receipt.TenantID, receipt.ID, receipt.Fingerprint)
		if r.Error != nil {
			return safe(r.Error)
		}
		if r.RowsAffected == 1 {
			if e := handler(tx); e != nil {
				return e
			}
			result = inbox.Applied
			return nil
		}
		var rows []struct{ Fingerprint string }
		if e := db.Raw("SELECT fingerprint FROM "+s.inbox+" WHERE namespace=? AND consumer=? AND tenant_id=? AND message_id=?", receipt.Namespace, receipt.Consumer, receipt.TenantID, receipt.ID).Scan(&rows).Error; e != nil {
			return safe(e)
		}
		if len(rows) != 1 || rows[0].Fingerprint != receipt.Fingerprint {
			return messaging.ErrIdentityConflict
		}
		result = inbox.Duplicate
		return nil
	})
	if err != nil {
		return "", safe(err)
	}
	return result, nil
}

type row struct {
	Seq                            int64
	Namespace, TenantID, MessageID string
	Kind                           messaging.Kind
	Type                           string
	SchemaVersion                  int
	Payload                        []byte
	Headers                        []byte
	OrderingKey, ConcurrencyKey    string
	ScheduledAt                    *time.Time
	AvailableAt                    time.Time
	State                          outbox.State
	Attempts                       int
	ClaimToken                     string
	LeaseUntil                     time.Time
	CreatedAt                      time.Time
	PublishedAt, FailedAt          *time.Time
	ErrorCode                      string
	ReplayCount                    int
	ReplayedAt                     *time.Time
}

func (r row) message() (messaging.Message, error) {
	m := messaging.Message{ID: r.MessageID, Namespace: r.Namespace, TenantID: r.TenantID, Kind: r.Kind, Type: r.Type, SchemaVersion: r.SchemaVersion,
		Payload: r.Payload, OrderingKey: r.OrderingKey, ConcurrencyKey: r.ConcurrencyKey, AvailableAt: r.AvailableAt}
	if r.ScheduledAt == nil {
		m.AvailableAt = time.Time{}
	} else {
		m.AvailableAt = *r.ScheduledAt
	}
	if err := json.Unmarshal(r.Headers, &m.Headers); err != nil {
		return m, messaging.ErrInvalidMessage
	}
	return m, m.Validate()
}
func (r row) record() (outbox.Record, error) {
	m, err := r.message()
	return outbox.Record{Message: m, State: r.State, Attempts: r.Attempts, CreatedAt: r.CreatedAt, PublishedAt: r.PublishedAt, FailedAt: r.FailedAt,
		ErrorCode: r.ErrorCode, ReplayCount: r.ReplayCount, ReplayedAt: r.ReplayedAt}, err
}

func (s *Store) Claim(ctx context.Context, req outbox.ClaimRequest) ([]outbox.Claim, error) {
	if req.Limit < 1 || req.Limit > 1000 || req.MaxAttempts < 1 || req.MaxAttempts > 1000000 || req.Lease < time.Millisecond || req.Lease > 24*time.Hour {
		return nil, messaging.ErrInvalidConfig
	}
	var claims []outbox.Claim
	err := s.transaction(ctx, func(db *gorm.DB) error {
		// Exhaustion recovery is bounded and uses SKIP LOCKED too, so a crashed
		// last-attempt worker cannot strand a processing row permanently.
		if err := db.Exec(`WITH exhausted AS (SELECT seq FROM `+s.outbox+` WHERE namespace=? AND attempts>=?
 AND (state='pending' OR (state='processing' AND lease_until<=clock_timestamp())) ORDER BY seq LIMIT ? FOR UPDATE SKIP LOCKED)
 UPDATE `+s.outbox+` o SET state='dead',failed_at=clock_timestamp(),error_code='attempts_exhausted',claim_token=NULL,lease_until=NULL
 FROM exhausted e WHERE o.seq=e.seq`, s.cfg.Namespace, req.MaxAttempts, req.Limit).Error; err != nil {
			return err
		}
		var rows []row
		if err := db.Raw(`WITH ready AS (SELECT seq FROM `+s.outbox+` WHERE namespace=? AND attempts<? AND
 ((state='pending' AND available_at<=clock_timestamp()) OR (state='processing' AND lease_until<=clock_timestamp()))
 ORDER BY available_at,seq LIMIT ? FOR UPDATE SKIP LOCKED)
 UPDATE `+s.outbox+` o SET state='processing',attempts=attempts+1,claim_token=? || ':' || o.seq::text,lease_until=clock_timestamp()+(? * interval '1 millisecond')
 FROM ready r WHERE o.seq=r.seq RETURNING o.*`, s.cfg.Namespace, req.MaxAttempts, req.Limit, uuid.NewString(), req.Lease.Milliseconds()).Scan(&rows).Error; err != nil {
			return err
		}
		for _, r := range rows {
			m, e := r.message()
			if e != nil {
				return e
			}
			claims = append(claims, outbox.Claim{Message: m, Token: r.ClaimToken, Attempt: r.Attempts, LeaseUntil: r.LeaseUntil})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claims, nil
}
func (s *Store) settle(ctx context.Context, c outbox.Claim, assignment string, args ...any) error {
	if c.Message.Namespace != s.cfg.Namespace || !messaging.ValidIdentity(c.Message.ID, false) || !messaging.ValidIdentity(c.Message.TenantID, true) || c.Token == "" {
		return messaging.ErrInvalidMessage
	}
	return s.transaction(ctx, func(db *gorm.DB) error {
		args = append(args, s.cfg.Namespace, c.Message.TenantID, c.Message.ID, c.Token)
		r := db.Exec("UPDATE "+s.outbox+" SET "+assignment+",claim_token=NULL,lease_until=NULL WHERE namespace=? AND tenant_id=? AND message_id=? AND claim_token=? AND state='processing' AND lease_until>clock_timestamp()", args...)
		if r.Error != nil {
			return r.Error
		}
		if r.RowsAffected != 1 {
			return messaging.ErrLeaseLost
		}
		return nil
	})
}
func (s *Store) MarkPublished(ctx context.Context, c outbox.Claim) error {
	return s.settle(ctx, c, "state='published',published_at=clock_timestamp(),error_code=''")
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
	return s.settle(ctx, c, "state='pending',available_at=clock_timestamp()+(? * interval '1 millisecond'),error_code=?", delay.Milliseconds(), code)
}
func (s *Store) Quarantine(ctx context.Context, c outbox.Claim, code string) error {
	if !validCode(code) {
		return messaging.ErrInvalidConfig
	}
	return s.settle(ctx, c, "state='dead',failed_at=clock_timestamp(),error_code=?", code)
}

var _ outbox.Store = (*Store)(nil)
var _ inbox.Processor[*orm.Transaction] = (*Store)(nil)

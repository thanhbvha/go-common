// Package mysql implements transactional messaging on Oracle MySQL 8.4/InnoDB.
// The application owns its ORM client and deployment migration lifecycle.
package mysql

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

	driver "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	"github.com/thanhbvha/go-common/v2/db/orm"
	"gorm.io/gorm"
)

//go:embed migrations/*.sql
var migrations embed.FS
var databasePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,63}$`)

// MigrationFS returns versioned SQL for the chosen database, consumable by
// db/migration. The caller runs Up in one deployment job, never service startup.
// Goose version 202611040002 must be included in the application's migration
// history or managed with a migration-only pool selecting this database.
func MigrationFS(database string) (fs.FS, error) {
	if !databasePattern.MatchString(database) {
		return nil, messaging.ErrInvalidConfig
	}
	b, err := migrations.ReadFile("migrations/202611040002_messaging.sql")
	if err != nil {
		return nil, err
	}
	return migrationFS{data: []byte(strings.ReplaceAll(string(b), "{{database}}", "`"+database+"`"))}, nil
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
	if name != "202611040002_messaging.sql" {
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
	Namespace, Database string
	Observer            messaging.Observer
}
type Store struct {
	client                 *orm.Client
	cfg                    Config
	outbox, inbox, version string
}

// Error preserves classification, including orm.ErrCommitUnknown, without
// rendering SQL, payloads or native errors in logs.
type Error struct{ cause error }

func (*Error) Error() string   { return "messaging mysql: database operation failed" }
func (e *Error) Unwrap() error { return e.cause }
func safe(err error) error {
	if err == nil {
		return nil
	}
	var native *driver.MySQLError
	if errors.As(err, &native) && (native.Number == 1146 || native.Number == 1054 || native.Number == 1049) {
		err = errors.Join(messaging.ErrSchema, err)
	}
	return &Error{cause: err}
}

func New(ctx context.Context, client *orm.Client, cfg Config) (*Store, error) {
	if client == nil || client.Capabilities().Name != "mysql" || !messaging.ValidIdentity(cfg.Namespace, false) || !databasePattern.MatchString(cfg.Database) {
		return nil, messaging.ErrInvalidConfig
	}
	prefix := "`" + cfg.Database + "`."
	s := &Store{client: client, cfg: cfg, outbox: prefix + "outbox", inbox: prefix + "inbox", version: prefix + "messaging_schema"}
	err := client.WithTransaction(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelReadCommitted}, func(tx *orm.Transaction) error {
		db, err := tx.DB()
		if err != nil {
			return err
		}
		var versions []int
		if err := db.Raw("SELECT version FROM " + s.version + " WHERE singleton=true").Scan(&versions).Error; err != nil {
			return errors.Join(messaging.ErrSchema, err)
		}
		if len(versions) != 1 || versions[0] != 1 {
			return messaging.ErrSchema
		}
		var tables []struct{ TableName, Engine string }
		if err := db.Raw("SELECT table_name,engine FROM information_schema.tables WHERE table_schema=? AND table_name IN ('outbox','inbox','messaging_schema')", cfg.Database).Scan(&tables).Error; err != nil {
			return err
		}
		if len(tables) != 3 {
			return messaging.ErrSchema
		}
		for _, table := range tables {
			if table.Engine != "InnoDB" {
				return messaging.ErrSchema
			}
		}
		if err := db.Exec("SELECT seq,namespace,tenant_id,message_id,kind,type,schema_version,payload,headers,ordering_key,concurrency_key,fingerprint,immutable_hash,scheduled_at,available_at,created_at,state,attempts,claim_token,lease_until,published_at,failed_at,error_code,replay_count,replayed_at FROM " + s.outbox + " LIMIT 0").Error; err != nil {
			return errors.Join(messaging.ErrSchema, err)
		}
		if err := db.Exec("SELECT seq,namespace,consumer,tenant_id,message_id,fingerprint,insert_token,processed_at FROM " + s.inbox + " LIMIT 0").Error; err != nil {
			return errors.Join(messaging.ErrSchema, err)
		}
		return s.validateIndexes(db)
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

// Append uses a locking read even in a caller's REPEATABLE READ transaction:
// its snapshot may predate an insert that won the identity race.
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
	headerBytes, err := json.Marshal(m.Headers)
	if err != nil {
		return messaging.ErrInvalidMessage
	}
	var scheduled *time.Time
	var storedSchedule *string
	var available *string
	if !m.AvailableAt.IsZero() {
		v := m.AvailableAt.UTC().Truncate(time.Microsecond)
		scheduled = &v
		text := v.Format(time.RFC3339Nano)
		storedSchedule = &text
		// DATETIME starts at year 1000. Keep earlier original schedules verbatim;
		// their dispatch time can safely be clamped because they are already due.
		dispatch := v
		if dispatch.Year() < 1000 {
			dispatch = time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)
		}
		formatted := sqlTime(dispatch)
		available = &formatted
	}
	immutable, err := json.Marshal([]any{fingerprint, m.Headers, m.OrderingKey, m.ConcurrencyKey, scheduled})
	if err != nil {
		return messaging.ErrInvalidMessage
	}
	h := sha256.Sum256(immutable)
	hash := hex.EncodeToString(h[:])
	db, err := tx.DB()
	if err != nil {
		return err
	}
	r := db.Exec("INSERT INTO "+s.outbox+` (namespace,tenant_id,message_id,kind,type,schema_version,payload,headers,ordering_key,concurrency_key,fingerprint,immutable_hash,scheduled_at,available_at,created_at)
 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,COALESCE(?,UTC_TIMESTAMP(6)),UTC_TIMESTAMP(6))
 ON DUPLICATE KEY UPDATE immutable_hash=immutable_hash`, m.Namespace, m.TenantID, m.ID, m.Kind, m.Type, m.SchemaVersion, []byte(m.Payload), string(headerBytes), m.OrderingKey, m.ConcurrencyKey, fingerprint, hash, storedSchedule, available)
	if r.Error != nil {
		return safe(r.Error)
	}
	var rows []struct{ ImmutableHash string }
	if err := db.Raw("SELECT immutable_hash FROM "+s.outbox+" WHERE namespace=? AND tenant_id=? AND message_id=? FOR UPDATE", m.Namespace, m.TenantID, m.ID).Scan(&rows).Error; err != nil {
		return safe(err)
	}
	if len(rows) != 1 || rows[0].ImmutableHash != hash {
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
		token := uuid.NewString()
		r := db.Exec("INSERT INTO "+s.inbox+` (namespace,consumer,tenant_id,message_id,fingerprint,insert_token,processed_at) VALUES (?,?,?,?,?,?,UTC_TIMESTAMP(6))
 ON DUPLICATE KEY UPDATE fingerprint=fingerprint`, receipt.Namespace, receipt.Consumer, receipt.TenantID, receipt.ID, receipt.Fingerprint, token)
		if r.Error != nil {
			return r.Error
		}
		var rows []struct{ Fingerprint, InsertToken string }
		if e := db.Raw("SELECT fingerprint,insert_token FROM "+s.inbox+" WHERE namespace=? AND consumer=? AND tenant_id=? AND message_id=? FOR UPDATE", receipt.Namespace, receipt.Consumer, receipt.TenantID, receipt.ID).Scan(&rows).Error; e != nil {
			return e
		}
		if len(rows) != 1 || rows[0].Fingerprint != receipt.Fingerprint {
			return messaging.ErrIdentityConflict
		}
		if rows[0].InsertToken != token {
			result = inbox.Duplicate
			return nil
		}
		if e := handler(tx); e != nil {
			return e
		}
		result = inbox.Applied
		return nil
	})
	if err != nil {
		return "", safe(err)
	}
	return result, nil
}

// DATETIME columns contain UTC wall-clock values regardless of session time_zone
// and the driver's DSN loc. Never let DSN location alter stored scheduling times.
func sqlTime(t time.Time) string {
	return t.UTC().Truncate(time.Microsecond).Format("2006-01-02 15:04:05.000000")
}
func utcWall(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}
func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := utcWall(*t)
	return &v
}

type row struct {
	Seq                            int64
	Namespace, TenantID, MessageID string
	Kind                           messaging.Kind
	Type                           string
	SchemaVersion                  int
	Payload, Headers               []byte
	OrderingKey, ConcurrencyKey    string
	ScheduledAt                    *string
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
	m := messaging.Message{ID: r.MessageID, Namespace: r.Namespace, TenantID: r.TenantID, Kind: r.Kind, Type: r.Type, SchemaVersion: r.SchemaVersion, Payload: r.Payload, OrderingKey: r.OrderingKey, ConcurrencyKey: r.ConcurrencyKey}
	if r.ScheduledAt != nil {
		v, e := time.Parse(time.RFC3339Nano, *r.ScheduledAt)
		if e != nil {
			return m, messaging.ErrInvalidMessage
		}
		m.AvailableAt = v
	}
	if err := json.Unmarshal(r.Headers, &m.Headers); err != nil {
		return m, messaging.ErrInvalidMessage
	}
	return m, m.Validate()
}
func (r row) record() (outbox.Record, error) {
	m, err := r.message()
	return outbox.Record{Message: m, State: r.State, Attempts: r.Attempts, CreatedAt: utcWall(r.CreatedAt), PublishedAt: utcPtr(r.PublishedAt), FailedAt: utcPtr(r.FailedAt), ErrorCode: r.ErrorCode, ReplayCount: r.ReplayCount, ReplayedAt: utcPtr(r.ReplayedAt)}, err
}

var _ outbox.Store = (*Store)(nil)
var _ inbox.Processor[*orm.Transaction] = (*Store)(nil)

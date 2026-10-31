package orm

import (
	"context"
	"database/sql/driver"
	"math"
	"reflect"
	"strconv"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type source interface {
	baseDB() *gorm.DB
	owner() *Client
	session(context.Context) (*gorm.DB, context.CancelFunc, error)
}

// Scope is trusted application code, never a query expression from an HTTP
// request. It may add tenant/ownership predicates. Inserts require application
// validation of ownership fields; a WHERE scope cannot authorize an INSERT.
type Scope func(*gorm.DB) *gorm.DB

type RepositoryOptions struct {
	WritableColumns []string
	SortableColumns []string
	Scope           Scope
	BatchSize       int
	MaxPageSize     int
	MaxOffset       int
	MaxBulkRows     int
	// VersionColumn enables mandatory CAS patching. It cannot be writable through
	// PatchByID or generic upsert. Delete/Restore retain their explicit ID contract.
	VersionColumn string
}

// Repository supports struct models with one scalar primary key. It is immutable
// after construction and reusable concurrently, except inside a Transaction.
// Native GORM features (joins, aggregates, raw SQL, hard delete) belong in the
// application's typed repositories via Client.DB / Transaction.DB.
type Repository[T any] struct {
	source   source
	schema   *schema.Schema
	pk       string
	deleted  string
	writable map[string]bool
	sortable map[string]bool
	opts     RepositoryOptions
}

func NewRepository[T any](c *Client, opts RepositoryOptions) (*Repository[T], error) {
	if c == nil {
		return nil, ErrInvalidArgument
	}
	return newRepository[T](c, opts)
}

// NewTransactionRepository creates an entirely new policy inside a transaction.
// Prefer rootRepository.InTransaction(tx) when a root policy already exists;
// this low-level constructor cannot infer its tenant scope or intended client.
func NewTransactionRepository[T any](tx *Transaction, opts RepositoryOptions) (*Repository[T], error) {
	if tx == nil {
		return nil, ErrInvalidArgument
	}
	if !tx.active.Load() {
		return nil, ErrTransactionClosed
	}
	return newRepository[T](tx, opts)
}

// InTransaction retains the compiled model, column policies, tenant Scope and
// resource limits of this root repository. The transaction must own its client.
func (r *Repository[T]) InTransaction(tx *Transaction) (*Repository[T], error) {
	if tx == nil {
		return nil, ErrInvalidArgument
	}
	if !tx.active.Load() {
		return nil, ErrTransactionClosed
	}
	if r.source.owner() != tx.client {
		return nil, ErrTransactionClientMismatch
	}
	if previous, ok := r.source.(*Transaction); ok {
		if !previous.active.Load() {
			return nil, ErrTransactionClosed
		}
		return nil, ErrNestedTransaction
	}
	clone := *r
	clone.source = tx
	return &clone, nil
}
func newRepository[T any](s source, opts RepositoryOptions) (*Repository[T], error) {
	t := reflect.TypeOf((*T)(nil)).Elem()
	if t.Kind() != reflect.Struct {
		return nil, invalid("model_must_be_struct")
	}
	stmt := &gorm.Statement{DB: s.baseDB()}
	if err := stmt.Parse(new(T)); err != nil {
		return nil, normalize("model", err)
	}
	if len(stmt.Schema.PrimaryFields) != 1 {
		return nil, invalid("single_primary_key_required")
	}
	pk := stmt.Schema.PrimaryFields[0]
	if pk.DataType != schema.Int && pk.DataType != schema.Uint && pk.DataType != schema.String {
		return nil, invalid("primary_key_type")
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 100
	}
	if opts.MaxPageSize == 0 {
		opts.MaxPageSize = 100
	}
	if opts.MaxOffset == 0 {
		opts.MaxOffset = 100000
	}
	if opts.MaxBulkRows == 0 {
		opts.MaxBulkRows = 10000
	}
	if opts.MaxBulkRows < 1 || opts.MaxBulkRows > 1000000 {
		return nil, invalid("max_bulk_rows")
	}
	if opts.BatchSize < 1 || opts.BatchSize > 1000 {
		return nil, invalid("batch_size")
	}
	if opts.MaxPageSize < 1 || opts.MaxPageSize > 1000 {
		return nil, invalid("max_page_size")
	}
	if opts.MaxOffset < 1 || opts.MaxOffset > 1000000 {
		return nil, invalid("max_offset")
	}
	// Leave headroom under the supported drivers' bind-variable limits, including
	// wide models. Associations are omitted, so only model columns participate.
	bindBudget := s.owner().capabilities.MaxBindParameters
	columns := len(stmt.Schema.DBNames)
	if columns == 0 || columns > bindBudget {
		return nil, invalid("model_columns")
	}
	if maxBatch := bindBudget / columns; opts.BatchSize > maxBatch {
		opts.BatchSize = maxBatch
	}
	r := &Repository[T]{source: s, schema: stmt.Schema, pk: pk.DBName, opts: opts, writable: map[string]bool{}, sortable: map[string]bool{pk.DBName: true}}
	// Only GORM's standard soft delete type is supported by Restore. Its physical
	// column name comes from schema metadata, including custom column tags.
	for _, f := range stmt.Schema.Fields {
		if f.IndirectFieldType == reflect.TypeOf(gorm.DeletedAt{}) {
			r.deleted = f.DBName
		}
	}
	if opts.VersionColumn != "" {
		f := stmt.Schema.FieldsByDBName[opts.VersionColumn]
		if f == nil || !f.Updatable || f.PrimaryKey || f.AutoCreateTime != 0 || f.AutoUpdateTime != 0 || opts.VersionColumn == r.deleted || (f.DataType != schema.Int && f.DataType != schema.Uint) {
			return nil, invalid("version_column")
		}
		kind := f.IndirectFieldType.Kind()
		if kind < reflect.Int || kind > reflect.Int64 {
			if kind < reflect.Uint || kind > reflect.Uint64 {
				return nil, invalid("version_column_type")
			}
		}
	}
	for _, name := range opts.WritableColumns {
		f := stmt.Schema.FieldsByDBName[name]
		if f == nil || !f.Updatable || f.PrimaryKey || f.AutoCreateTime != 0 || f.AutoUpdateTime != 0 || name == r.deleted || name == opts.VersionColumn {
			return nil, invalid("writable_columns")
		}
		r.writable[name] = true
	}
	for _, name := range opts.SortableColumns {
		f := stmt.Schema.FieldsByDBName[name]
		if f == nil || !f.Readable {
			return nil, invalid("sortable_columns")
		}
		r.sortable[name] = true
	}
	// Copy caller slices; mutating options after construction cannot alter policy.
	r.opts.WritableColumns = append([]string(nil), opts.WritableColumns...)
	r.opts.SortableColumns = append([]string(nil), opts.SortableColumns...)
	return r, nil
}
func (r *Repository[T]) resultError(op string, db *gorm.DB) error {
	return r.source.owner().resultError(op, db)
}
func (r *Repository[T]) query(ctx context.Context) (*gorm.DB, context.CancelFunc, error) {
	db, cancel, err := r.source.session(ctx)
	if err != nil {
		return nil, nil, err
	}
	db = db.Model(new(T))
	if r.opts.Scope != nil {
		db = r.opts.Scope(db)
		if db == nil {
			cancel()
			return nil, nil, ErrInvalidArgument
		}
	}
	return db, cancel, nil
}

// scalarValue rejects SQL expressions and containers, including GORM's dynamic
// expression interfaces. driver.Valuer results are checked before binding.
func scalarValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if _, ok := value.(clause.Expression); ok {
		return nil, ErrInvalidArgument
	}
	if _, ok := value.(gorm.Valuer); ok {
		return nil, ErrInvalidArgument
	}
	if v, ok := value.(driver.Valuer); ok {
		out, err := v.Value()
		if err != nil {
			return nil, normalize("value", err)
		}
		value = out
		if value == nil {
			return nil, nil
		}
	}
	if _, ok := value.(time.Time); ok {
		return value, nil
	}
	if b, ok := value.([]byte); ok {
		return b, nil
	}
	// Strip custom dynamic-expression methods from named scalar types returned
	// by Valuer implementations before passing them to GORM.
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.String:
		return v.String(), nil
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint(), nil
	case reflect.Float32, reflect.Float64:
		return v.Float(), nil
	}
	return nil, ErrInvalidArgument
}
func (r *Repository[T]) identity(id any) (clause.Expression, error) {
	value, err := scalarValue(id)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, ErrInvalidArgument
	}
	pk := r.schema.PrimaryFields[0]
	if pk.DataType == schema.String {
		if reflect.TypeOf(value).Kind() != reflect.String {
			return nil, ErrInvalidArgument
		}
	} else {
		// MySQL can otherwise coerce a malformed string such as "1suffix" to ID 1.
		bits := pk.Size
		if bits <= 0 || bits > 64 {
			bits = 64
		}
		var text string
		switch v := reflect.ValueOf(value); v.Kind() {
		case reflect.String:
			if v.Len() > 4096 {
				return nil, ErrInvalidArgument
			}
			text = v.String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			text = strconv.FormatInt(v.Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			text = strconv.FormatUint(v.Uint(), 10)
		default:
			return nil, ErrInvalidArgument
		}
		if pk.DataType == schema.Uint {
			n, e := strconv.ParseUint(text, 10, bits)
			if e != nil || n == 0 {
				return nil, ErrInvalidArgument
			}
			value = n
		} else {
			n, e := strconv.ParseInt(text, 10, bits)
			if e != nil || n <= 0 {
				return nil, ErrInvalidArgument
			}
			value = n
		}
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.String:
		if v.Len() == 0 || v.Len() > 4096 {
			return nil, ErrInvalidArgument
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v.Int() <= 0 {
			return nil, ErrInvalidArgument
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if v.Uint() == 0 {
			return nil, ErrInvalidArgument
		}
	default:
		return nil, ErrInvalidArgument
	}
	return clause.Eq{Column: clause.Column{Table: clause.CurrentTable, Name: r.pk}, Value: value}, nil
}
func (r *Repository[T]) FindByID(ctx context.Context, id any) (*T, error) {
	condition, err := r.identity(id)
	if err != nil {
		return nil, err
	}
	db, cancel, err := r.query(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	var result T
	if err := r.resultError("find_by_id", db.Where(condition).Take(&result)); err != nil {
		return nil, err
	}
	return &result, nil
}

// FindOne evaluates trusted scopes. Values from outside the application must be
// passed through GORM placeholders inside those scopes.
func (r *Repository[T]) FindOne(ctx context.Context, filters ...Scope) (*T, error) {
	db, cancel, err := r.query(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	db, err = applyFilters(db, filters)
	if err != nil {
		return nil, err
	}
	var result T
	if err := r.resultError("find_one", db.Order(clause.OrderByColumn{Column: clause.Column{Name: r.pk}}).First(&result)); err != nil {
		return nil, err
	}
	return &result, nil
}
func applyFilters(db *gorm.DB, filters []Scope) (*gorm.DB, error) {
	for _, f := range filters {
		if f == nil {
			return nil, ErrInvalidArgument
		}
		db = f(db)
		if db == nil {
			return nil, ErrInvalidArgument
		}
	}
	return db, nil
}
func (r *Repository[T]) Create(ctx context.Context, model *T) error {
	if model == nil {
		return ErrInvalidArgument
	}
	if err := r.createIdentity(ctx, model); err != nil {
		return err
	}
	db, cancel, err := r.query(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return r.resultError("create", db.Omit(clause.Associations).Create(model))
}

// CreateMany splits SQL statements into bounded batches. All batches participate
// in one transaction; callers must discard generated IDs on any error.
func (r *Repository[T]) CreateMany(ctx context.Context, models []T) error {
	if err := ctx.Err(); err != nil {
		return normalize("create_many", err)
	}
	if len(models) > r.opts.MaxBulkRows {
		return ErrInvalidArgument
	}
	if len(models) == 0 {
		return nil
	}
	db, cancel, err := r.query(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	if err := r.validateBulk(db.Statement.Context, models); err != nil {
		return err
	}
	return r.createBatches("create_many", db, models)
}

func (r *Repository[T]) validateBulk(ctx context.Context, models []T) error {
	for i := range models {
		if err := ctx.Err(); err != nil {
			return normalize("bulk_validation", err)
		}
		if err := r.createIdentity(ctx, &models[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository[T]) createIdentity(ctx context.Context, model *T) error {
	pk := r.schema.PrimaryFields[0]
	value, zero := pk.ValueOf(ctx, reflect.ValueOf(model))
	if zero && (pk.AutoIncrement || pk.HasDefaultValue) {
		return nil
	}
	_, err := r.identity(value)
	return err
}

// PatchByID updates exactly the supplied columns, including zero and NULL values.
// Empty patches, PK/soft-delete changes, and non-allowlisted columns are rejected.
// No insert fallback occurs. Zero affected rows returns ErrNotFound (PostgreSQL).
func (r *Repository[T]) PatchByID(ctx context.Context, id any, values map[string]any) error {
	if r.opts.VersionColumn != "" {
		return ErrVersionRequired
	}
	return r.patch(ctx, id, values, nil)
}

// CompareAndSwapByID atomically checks/increments the version with the patch.
// Zero matched rows means ErrVersionConflict (also ErrConflict), including a
// missing/inaccessible row; no existence probe leaks rows outside the scope.
func (r *Repository[T]) CompareAndSwapByID(ctx context.Context, id any, expected uint64, values map[string]any) error {
	name := r.opts.VersionColumn
	if name == "" {
		return ErrInvalidArgument
	}
	f := r.schema.FieldsByDBName[name]
	bits := f.IndirectFieldType.Bits()
	maxVersion := uint64(math.MaxInt64)
	if bits < 64 {
		if kind := f.IndirectFieldType.Kind(); kind >= reflect.Int && kind <= reflect.Int64 {
			maxVersion = (uint64(1) << (bits - 1)) - 1
		} else {
			maxVersion = (uint64(1) << bits) - 1
		}
	}
	if expected >= maxVersion {
		return ErrInvalidArgument
	}
	return r.patch(ctx, id, values, &expected)
}

func (r *Repository[T]) patch(ctx context.Context, id any, values map[string]any, version *uint64) error {
	condition, err := r.identity(id)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return ErrInvalidArgument
	}
	safe := make(map[string]any, len(values))
	for name, value := range values {
		if !r.writable[name] {
			return ErrInvalidArgument
		}
		v, e := scalarValue(value)
		if e != nil {
			return e
		}
		safe[name] = v
	}
	db, cancel, err := r.query(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	if version != nil {
		name := r.opts.VersionColumn
		db = db.Where(clause.Eq{Column: clause.Column{Table: clause.CurrentTable, Name: name}, Value: *version})
		// This expression is library-generated from a validated schema column.
		safe[name] = gorm.Expr("? + ?", clause.Column{Name: name}, 1)
		result := db.Where(condition).Omit(clause.Associations).Updates(safe)
		if result.Error != nil {
			return r.resultError("compare_and_swap", result)
		}
		if result.RowsAffected == 0 {
			return &Error{Operation: "compare_and_swap", Kind: ErrConflict, cause: ErrVersionConflict}
		}
		return nil
	}
	return r.writeResult("patch", db.Where(condition).Omit(clause.Associations).Updates(safe))
}
func (r *Repository[T]) writeResult(op string, db *gorm.DB) error {
	if db.Error != nil {
		return r.resultError(op, db)
	}
	if db.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteByID uses GORM soft delete when available, otherwise a physical delete.
func (r *Repository[T]) DeleteByID(ctx context.Context, id any) error {
	condition, err := r.identity(id)
	if err != nil {
		return err
	}
	db, cancel, err := r.query(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return r.writeResult("delete", db.Where(condition).Delete(new(T)))
}

// RestoreByID restores only deleted records with gorm.DeletedAt. Authorization
// scopes still apply. Hard deletion and unrestricted queries are native APIs.
func (r *Repository[T]) RestoreByID(ctx context.Context, id any) error {
	if r.deleted == "" {
		return ErrInvalidArgument
	}
	condition, err := r.identity(id)
	if err != nil {
		return err
	}
	db, cancel, err := r.query(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return r.writeResult("restore", db.Unscoped().Where(condition).Where(clause.Neq{Column: clause.Column{Name: r.deleted}, Value: nil}).Update(r.deleted, nil))
}

// UpsertPolicy requires an explicit conflict target and either update columns or
// DoNothing. There is deliberately no UpdateAll default.
type UpsertPolicy struct {
	ConflictColumns []string
	UpdateColumns   []string
	DoNothing       bool
	// AnyUniqueKey explicitly opts into MySQL's ON DUPLICATE KEY semantics.
	// It cannot be combined with ConflictColumns or DoNothing.
	AnyUniqueKey bool
}

func (r *Repository[T]) conflict(policy UpsertPolicy) (clause.OnConflict, error) {
	caps := r.source.owner().capabilities
	if policy.AnyUniqueKey {
		if !caps.AnyUniqueKeyUpsert {
			return clause.OnConflict{}, ErrUnsupportedFeature
		}
		if caps.StrictConflictTarget || len(policy.ConflictColumns) > 0 || policy.DoNothing || len(policy.UpdateColumns) == 0 {
			return clause.OnConflict{}, ErrInvalidArgument
		}
	} else if !caps.StrictConflictTarget {
		return clause.OnConflict{}, ErrUnsupportedFeature
	}
	if (!policy.AnyUniqueKey && len(policy.ConflictColumns) == 0) || policy.DoNothing == (len(policy.UpdateColumns) > 0) {
		return clause.OnConflict{}, ErrInvalidArgument
	}
	if policy.DoNothing && !caps.ConflictDoNothing {
		return clause.OnConflict{}, ErrUnsupportedFeature
	}
	out := clause.OnConflict{DoNothing: policy.DoNothing}
	for _, name := range policy.ConflictColumns {
		if r.schema.FieldsByDBName[name] == nil {
			return out, ErrInvalidArgument
		}
		out.Columns = append(out.Columns, clause.Column{Name: name})
	}
	for _, name := range policy.UpdateColumns {
		if !r.writable[name] {
			return out, ErrInvalidArgument
		}
	}
	if !policy.DoNothing {
		out.DoUpdates = clause.AssignmentColumns(policy.UpdateColumns)
	}
	return out, nil
}

// UpsertMany returns rows affected by the driver. Zero is valid for DoNothing.
// Conflict targets must correspond to actual database unique constraints.
func (r *Repository[T]) UpsertMany(ctx context.Context, models []T, policy UpsertPolicy) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, normalize("upsert_many", err)
	}
	if r.opts.VersionColumn != "" {
		return 0, ErrVersionRequired
	}
	if len(models) > r.opts.MaxBulkRows {
		return 0, ErrInvalidArgument
	}
	// ON CONFLICT does not honor an ordinary WHERE authorization scope. A scoped
	// repository must use an application-specific upsert with a tenant-safe unique
	// target and conflict predicate instead of accidentally bypassing that scope.
	if r.opts.Scope != nil {
		return 0, ErrInvalidArgument
	}
	conflict, err := r.conflict(policy)
	if err != nil {
		return 0, err
	}
	if len(models) == 0 {
		return 0, nil
	}
	db, cancel, err := r.query(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()
	if err := r.validateBulk(db.Statement.Context, models); err != nil {
		return 0, err
	}
	var rows int64
	err = r.createBatches("upsert_many", db.Clauses(conflict), models, &rows)
	return rows, err
}

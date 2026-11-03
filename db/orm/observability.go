package orm

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/thanhbvha/go-common/v2/db/orm/backend"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// No SQL, parameters, identifiers or raw driver messages are emitted, including
// literal SQL and GORM initialization errors.
type safeLogger struct {
	logger  *slog.Logger
	slow    time.Duration
	level   gormlogger.LogLevel
	backend backend.Backend
}

func (l safeLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	l.level = level
	return l
}
func (l safeLogger) Info(ctx context.Context, _ string, _ ...any) {
	if l.level == gormlogger.Info {
		l.logger.InfoContext(ctx, "orm internal event")
	}
}
func (l safeLogger) Warn(ctx context.Context, _ string, _ ...any) {
	if l.level != gormlogger.Silent {
		l.logger.WarnContext(ctx, "orm internal warning")
	}
}
func (l safeLogger) Error(ctx context.Context, _ string, _ ...any) {
	if l.level != gormlogger.Silent {
		l.logger.ErrorContext(ctx, "orm internal error")
	}
}
func (l safeLogger) Trace(ctx context.Context, start time.Time, fc func() (string, int64), err error) {
	if l.level == gormlogger.Silent {
		return
	}
	elapsed := time.Since(start)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		l.logger.ErrorContext(ctx, "orm query failed", "duration", elapsed, "error", normalizeBackend("query", err, l.backend))
		return
	}
	if elapsed >= l.slow {
		_, rows := fc()
		l.logger.WarnContext(ctx, "orm slow query", "duration", elapsed, "rows", rows)
	}
}

type telemetry struct{ tracer trace.Tracer }
type traceContext struct {
	context.Context
	parent context.Context
	span   trace.Span
}

func newTelemetry(provider trace.TracerProvider) *telemetry {
	if provider == nil {
		provider = otel.GetTracerProvider()
	}
	return &telemetry{tracer: provider.Tracer("github.com/thanhbvha/go-common/v2/db/orm")}
}
func (*telemetry) Name() string { return "orm_metadata_telemetry" }
func (p *telemetry) Initialize(db *gorm.DB) error {
	before := func(op string) func(*gorm.DB) {
		return func(tx *gorm.DB) {
			parent := tx.Statement.Context
			ctx, span := p.tracer.Start(parent, "orm."+op, trace.WithSpanKind(trace.SpanKindClient))
			tx.Statement.Context = traceContext{Context: ctx, parent: parent, span: span}
		}
	}
	after := func(tx *gorm.DB) {
		ctx, ok := tx.Statement.Context.(traceContext)
		if !ok {
			return
		}
		defer ctx.span.End()
		defer func() { tx.Statement.Context = ctx.parent }()
		ctx.span.SetAttributes(attribute.Int64("db.rows_affected", tx.RowsAffected))
		if tx.Error != nil && !errors.Is(tx.Error, gorm.ErrRecordNotFound) {
			ctx.span.SetStatus(codes.Error, "database operation failed")
		}
	}
	// Instance-local hooks. Stats are exposed by Client, without global metric
	// registrations or reporting goroutines that outlive the pool.
	pairs := []struct{ a, b error }{
		{db.Callback().Create().Before("*").Register("orm:before", before("create")), db.Callback().Create().After("*").Register("orm:after", after)},
		{db.Callback().Query().Before("*").Register("orm:before", before("select")), db.Callback().Query().After("*").Register("orm:after", after)},
		{db.Callback().Update().Before("*").Register("orm:before", before("update")), db.Callback().Update().After("*").Register("orm:after", after)},
		{db.Callback().Delete().Before("*").Register("orm:before", before("delete")), db.Callback().Delete().After("*").Register("orm:after", after)},
		{db.Callback().Raw().Before("*").Register("orm:before", before("raw")), db.Callback().Raw().After("*").Register("orm:after", after)},
		{db.Callback().Row().Before("*").Register("orm:before", before("row")), db.Callback().Row().After("*").Register("orm:after", after)},
	}
	for _, pair := range pairs {
		if pair.a != nil {
			return pair.a
		}
		if pair.b != nil {
			return pair.b
		}
	}
	return nil
}

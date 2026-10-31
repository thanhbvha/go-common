package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/thanhbvha/go-common/db/orm"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func byExternalID(id string) orm.Scope {
	return func(db *gorm.DB) *gorm.DB { return db.Where("external_id = ?", id) }
}
func demoNative(ctx context.Context, opt options) error {
	return withFixture(ctx, opt, nil, func(f *fixture) error {
		r, err := f.repository()
		if err != nil {
			return err
		}
		rows := []Patient{patient("native-a", "one"), patient("native-b", "one")}
		if err := r.CreateMany(ctx, rows); err != nil {
			return err
		}
		visits, err := orm.NewRepository[Visit](f.client, orm.RepositoryOptions{})
		if err != nil {
			return err
		}
		if err := visits.Create(ctx, &Visit{PatientID: rows[0].ID, Note: "Synthetic note"}); err != nil {
			return err
		}
		query, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		db := f.client.DB().WithContext(query)
		var aggregate []struct {
			Status string
			Total  int64
		}
		if err := nativeFailure("aggregate", db.Model(&Patient{}).Select("status, count(*) AS total").Where("age >= ?", 18).Group("status").Scan(&aggregate).Error); err != nil {
			return err
		}
		var count int64
		// Table is an identifier quoted by GORM; 18 is a bound value. Neither comes
		// from concatenating request input into an SQL expression.
		if err := nativeFailure("raw count", db.Raw("SELECT count(*) FROM ? WHERE age >= ?", clause.Table{Name: f.prefix + "patients"}, 18).Scan(&count).Error); err != nil {
			return err
		}
		var joined []struct {
			PatientName string
			Note        string
		}
		err = nativeFailure("join", db.Table(f.prefix+"patients AS p").Select("p.name AS patient_name, v.note").Joins("JOIN ? AS v ON v.patient_id = p.id", clause.Table{Name: f.prefix + "visits"}).Where("p.tenant_id = ?", "one").Scan(&joined).Error)
		if err != nil {
			return err
		}
		fmt.Printf("  aggregate_groups=%d raw_count=%d joined_rows=%d\n", len(aggregate), count, len(joined))
		fmt.Println("  native GORM bypasses repository policy/limits; bound values and deadlines are explicit")
		return nil
	})
}
func demoErrors(ctx context.Context, opt options) error {
	return withFixture(ctx, opt, nil, func(f *fixture) error {
		r, err := f.repository()
		if err != nil {
			return err
		}
		row := patient("errors", "one")
		if err := r.Create(ctx, &row); err != nil {
			return err
		}
		if _, err := r.FindByID(ctx, "1suffix"); !errors.Is(err, orm.ErrInvalidArgument) {
			return fmt.Errorf("malformed numeric ID accepted")
		}
		if _, err := r.FindByID(ctx, 999999); !errors.Is(err, orm.ErrNotFound) {
			return fmt.Errorf("missing record contract lost")
		}
		if err := expect(r.PatchByID(ctx, row.ID, map[string]any{"external_id": "privileged"}), orm.ErrInvalidArgument); err != nil {
			return err
		}
		duplicate := patient("errors", "one")
		err = r.Create(ctx, &duplicate)
		if err := expect(err, orm.ErrConflict); err != nil {
			return err
		}
		describeError(err)
		var safe *orm.Error
		if !errors.As(err, &safe) {
			return fmt.Errorf("database error metadata missing")
		}
		// Inspect structured native fields without printing Message/Detail or raw cause.
		cause := safe.Unwrap()
		switch f.client.Capabilities().Name {
		case "postgres":
			var native *pgconn.PgError
			if !errors.As(cause, &native) {
				return fmt.Errorf("PostgreSQL error chain lost")
			}
			fmt.Printf("  native PostgreSQL code=%s\n", native.Code)
		case "mysql":
			var native *driver.MySQLError
			if !errors.As(cause, &native) {
				return fmt.Errorf("MySQL error chain lost")
			}
			fmt.Printf("  native MySQL number=%d\n", native.Number)
		case "sqlite":
			var native interface{ Code() int }
			if !errors.As(cause, &native) {
				return fmt.Errorf("SQLite error chain lost")
			}
			fmt.Printf("  native SQLite code=%d\n", native.Code())
		}
		classified := f.config.Backend.Classify(err)
		fmt.Printf("  backend classification=%s code=%s\n", classified.Kind, classified.Code)
		children, err := orm.NewRepository[Visit](f.client, orm.RepositoryOptions{})
		if err != nil {
			return err
		}
		if err := expect(children.Create(ctx, &Visit{PatientID: 999999}), orm.ErrConflict); err != nil {
			return err
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := r.FindByID(canceled, row.ID); !errors.Is(err, context.Canceled) {
			return fmt.Errorf("cancellation contract lost")
		}
		invalid := f.config
		invalid.MaxOpenConns = 0
		if err := expect(invalid.Validate(), orm.ErrInvalidConfig); err != nil {
			return err
		}
		_, err = r.FindOne(ctx, func(db *gorm.DB) *gorm.DB { return db.Where("nonexistent_example_column = ?", 1) })
		if err := expect(err, orm.ErrDatabase); err != nil {
			return err
		}
		describeError(err)
		fmt.Println("  classified errors are safe to print; raw driver messages/DSNs are never printed")
		return nil
	})
}

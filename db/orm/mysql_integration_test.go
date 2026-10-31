//go:build integration

package orm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	mysqlbackend "github.com/thanhbvha/go-common/db/orm/backend/mysql"
	"gorm.io/gorm/schema"
)

func mysqlTest(t *testing.T) *Client {
	t.Helper()
	dsn := os.Getenv("ORM_MYSQL_DSN")
	if dsn == "" {
		if os.Getenv("ORM_MYSQL_REQUIRED") == "1" {
			t.Fatal("ORM_MYSQL_DSN is required")
		}
		t.Skip("set ORM_MYSQL_DSN for disposable MySQL 8.4 tests")
	}
	cfg := DefaultConfig()
	cfg.Backend = mysqlbackend.New(dsn)
	cfg.MaxOpenConns = 2
	cfg.MaxIdleConns = 2
	c, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.db.Config.NamingStrategy = schema.NamingStrategy{TablePrefix: fmt.Sprintf("orm_%d_%d_", time.Now().UnixNano(), dbCounter.Add(1))}
	t.Cleanup(func() {
		if err := c.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return c
}
func TestMySQLContract(t *testing.T) { runContract(t, mysqlTest) }
func TestPostgresContract(t *testing.T) {
	runContract(t, func(t *testing.T) *Client { return postgresTest(t) })
}
func TestMySQLNativeErrorsAndRequirements(t *testing.T) {
	c := mysqlTest(t)
	r, err := NewRepository[contractRow](c, RepositoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.DB().AutoMigrate(&contractRow{}); err != nil {
		t.Fatal("mysql fixture failed")
	}
	defer c.DB().Migrator().DropTable(&contractRow{})
	row := contractRow{SKU: "duplicate", Alias: "duplicate"}
	if err := r.Create(context.Background(), &row); err != nil {
		t.Fatal(err)
	}
	err = r.Create(context.Background(), &contractRow{SKU: "duplicate", Alias: "different"})
	var native *driver.MySQLError
	var wrapped *Error
	if !errors.Is(err, ErrConflict) || !errors.As(err, &native) || native.Number != 1062 || !errors.As(err, &wrapped) || wrapped.DriverCode != "1062" || wrapped.SQLState != "23000" {
		t.Fatal("mysql native error chain lost")
	}
	if strings.Contains(err.Error(), "duplicate") {
		t.Fatal("driver data leaked")
	}
	cfg := DefaultConfig()
	cfg.Backend = c.backend
	cfg.Require.TransactionLocalTimeouts = true
	if err := cfg.Validate(); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatal("unsupported timeout accepted")
	}
}

func TestMySQLRejectsNonTransactionalTables(t *testing.T) {
	c := mysqlTest(t)
	table := fmt.Sprintf("orm_myisam_%d", dbCounter.Add(1))
	if err := c.DB().Exec("CREATE TABLE " + table + " (id bigint PRIMARY KEY) ENGINE=MyISAM").Error; err != nil {
		t.Fatal("create nontransactional fixture failed")
	}
	defer c.DB().Exec("DROP TABLE " + table)
	cfg := DefaultConfig()
	cfg.Backend = c.backend
	opened, err := Open(context.Background(), cfg)
	if opened != nil {
		_ = opened.Close(context.Background())
	}
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatal("nontransactional tables were silently accepted")
	}
}

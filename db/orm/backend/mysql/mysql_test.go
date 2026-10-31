package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	driver "github.com/go-sql-driver/mysql"
	"github.com/thanhbvha/go-common/db/orm/backend"
)

func TestPrepareOwnsSafeDriverSettings(t *testing.T) {
	b := New("user@tcp(127.0.0.1:1)/app?clientFoundRows=false&parseTime=false")
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	d, err := b.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cfg := d.(*ownedDialector).Config
	defer cfg.Conn.(*sql.DB).Close()
	if !cfg.SkipInitializeWithVersion || !cfg.DSNConfig.ClientFoundRows || !cfg.DSNConfig.ParseTime {
		t.Fatal("driver contract not enforced")
	}
	for _, dsn := range []string{"bad", "user@/app?multiStatements=true", "user@/app?interpolateParams=true", "user@/app?sql_mode=invalid"} {
		if err := New(dsn).Validate(); !errors.Is(err, backend.ErrInvalidConfig) {
			t.Fatal("unsafe configuration accepted")
		}
	}
}
func TestClassifyPreservesNativeError(t *testing.T) {
	b := New("unused")
	native := &driver.MySQLError{Number: 1062, SQLState: [5]byte{'2', '3', '0', '0', '0'}, Message: "private-value"}
	info := b.Classify(native)
	if info.Kind != backend.Constraint || info.SQLState != "23000" || info.Code != "1062" {
		t.Fatalf("bad classification: %+v", info)
	}
}

package postgres

import (
	"io/fs"
	"testing"
	"testing/fstest"
)

func TestMigrationFilesystem(t *testing.T) {
	for _, schema := range []string{"public", "transactional", "a_1"} {
		files, err := MigrationFS(schema)
		if err != nil {
			t.Fatal(err)
		}
		if err := fstest.TestFS(files, "202611040001_messaging.sql"); err != nil {
			t.Fatal(err)
		}
		if _, err := fs.ReadFile(files, "../migrations/202611040001_messaging.sql"); err == nil {
			t.Fatal("traversal accepted")
		}
	}
	for _, schema := range []string{"", "public;DROP SCHEMA public", "a.b", "Upper", "bad\"identifier"} {
		if _, err := MigrationFS(schema); err == nil {
			t.Fatal("invalid schema accepted")
		}
	}
}

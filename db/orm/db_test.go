package orm_test

import (
	"context"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/thanhbvha/go-common/db/orm"
	"gorm.io/gorm"
)

type User struct {
	gorm.Model
	Name string
	Age  int
}

func setupTestDB(t *testing.T) *gorm.DB {
	// Use SQLite in-memory for testing
	gormDB, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("Failed to connect to test db: %v", err)
	}

	// Migrate the schema
	if err := gormDB.AutoMigrate(&User{}); err != nil {
		t.Fatalf("Failed to migrate: %v", err)
	}

	// Clear table just in case (shared cache)
	gormDB.Where("1 = 1").Delete(&User{})

	// Seed data
	users := []User{
		{Name: "Alice", Age: 30},
		{Name: "Bob", Age: 25},
		{Name: "Charlie", Age: 35},
		{Name: "Diana", Age: 28},
		{Name: "Eve", Age: 22},
	}
	gormDB.Create(&users)

	return gormDB
}

func TestPagination(t *testing.T) {
	gormDB := setupTestDB(t)

	// Test Page 1, Size 2, Sort by age ASC
	req := orm.PageRequest{Page: 1, Size: 2, Sorts: []string{"age asc"}}
	resp, err := orm.ExecutePagination[User](gormDB.Model(&User{}), req)
	if err != nil {
		t.Fatalf("ExecutePagination failed: %v", err)
	}

	if resp.TotalRows != 5 {
		t.Errorf("Expected TotalRows 5, got %d", resp.TotalRows)
	}
	if resp.TotalPages != 3 { // 5 rows / 2 size = 2.5 -> 3 pages
		t.Errorf("Expected TotalPages 3, got %d", resp.TotalPages)
	}
	if len(resp.Items) != 2 {
		t.Errorf("Expected 2 items, got %d", len(resp.Items))
	}
	// Sorted by age asc: Eve(22), Bob(25)
	if resp.Items[0].Name != "Eve" || resp.Items[1].Name != "Bob" {
		t.Errorf("Unexpected sort order. Got %s, %s", resp.Items[0].Name, resp.Items[1].Name)
	}

	// Test Page 3 (should have 1 item)
	req2 := orm.PageRequest{Page: 3, Size: 2}
	resp2, err := orm.ExecutePagination[User](gormDB.Model(&User{}), req2)
	if err != nil {
		t.Fatalf("ExecutePagination failed: %v", err)
	}
	if len(resp2.Items) != 1 {
		t.Errorf("Expected 1 item on page 3, got %d", len(resp2.Items))
	}
}

func TestPaginationWithFilters(t *testing.T) {
	gormDB := setupTestDB(t)

	// Filter age > 25 (Alice(30), Charlie(35), Diana(28) -> 3 items)
	req := orm.PageRequest{Page: 1, Size: 10, Sorts: []string{"age desc"}}
	
	repo := orm.NewRepository[User](gormDB)
	resp, err := repo.Paginate(context.Background(), req, "age > ?", 25)
	if err != nil {
		t.Fatalf("ExecutePagination failed: %v", err)
	}

	if resp.TotalRows != 3 {
		t.Errorf("Expected TotalRows 3, got %d", resp.TotalRows)
	}
	if len(resp.Items) != 3 {
		t.Fatalf("Expected 3 items, got %d", len(resp.Items))
	}
	// Sorted by age desc: Charlie(35), Alice(30), Diana(28)
	if resp.Items[0].Name != "Charlie" {
		t.Errorf("Expected first item to be Charlie, got %s", resp.Items[0].Name)
	}
}

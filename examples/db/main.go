package main

import (
	"context"
	"fmt"

	"github.com/glebarez/sqlite"
	"github.com/thanhbvha/go-common/db/orm"
	"gorm.io/gorm"
)

type Patient struct {
	gorm.Model
	FullName string
	Age      int
	Status   string
}

func main() {
	fmt.Println("--- Database/ORM Example ---")

	// 1. In a real app, you would load this from config using the 'config' module
	// For this example, we'll use an in-memory SQLite connection directly
	dbConn, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		panic("failed to connect to db")
	}

	// 2. Auto-migrate schema
	dbConn.AutoMigrate(&Patient{})

	// Khởi tạo Repository trước
	patientRepo := orm.NewRepository[Patient](dbConn)

	// 3. Seed some dummy data using the Repository's InsertMany function
	err = patientRepo.InsertMany(context.Background(), []Patient{
		{FullName: "Nguyen Van A", Age: 45, Status: "WAITING"},
		{FullName: "Tran Thi B", Age: 32, Status: "IN_PROGRESS"},
		{FullName: "Le Van C", Age: 28, Status: "WAITING"},
		{FullName: "Pham Thi D", Age: 65, Status: "COMPLETED"},
		{FullName: "Hoang Van E", Age: 50, Status: "WAITING"},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("\n--- Fetching WAITING Patients (Page 1, Size 2) ---")

	req := orm.PageRequest{
		Page:  1,
		Size:  2,
		Sorts: []string{"age desc"},
	}

	// Execute pagination via Generic Repository
	resp, err := patientRepo.Paginate(context.Background(), req, "status = ?", "WAITING")
	if err != nil {
		panic(err)
	}

	// Print results
	fmt.Printf("Total Records: %d\n", resp.TotalRows)
	fmt.Printf("Total Pages: %d\n", resp.TotalPages)
	fmt.Printf("Current Page: %d\n", resp.Page)

	for i, p := range resp.Items {
		fmt.Printf("  %d. %s (Age: %d)\n", i+1, p.FullName, p.Age)
	}

	// ==========================================
	// THỐNG KÊ (STATISTICS) EXAMPLES
	// ==========================================

	type StatusCount struct {
		Status string
		Total  int
	}

	// 5. Example 1: Dùng Aggregate (Query Builder)
	fmt.Println("\n--- 1. Aggregate: Group by Status ---")
	var aggResults []StatusCount

	err = patientRepo.Aggregate(context.Background(), &aggResults, func(db *gorm.DB) *gorm.DB {
		return db.Select("status, count(*) as total").Group("status")
	})
	if err != nil {
		panic(err)
	}

	for _, res := range aggResults {
		fmt.Printf("  Status: %-12s | Total: %d\n", res.Status, res.Total)
	}

	// 6. Example 2: Dùng RawQuery (Manual SQL)
	fmt.Println("\n--- 2. RawQuery: Custom SQL (Age > 30) ---")
	var rawResults []StatusCount

	// Lưu ý: Tên bảng ở đây phải viết cứng ("patients") vì ta đang dùng SQL thuần
	sqlQuery := `
		SELECT status, count(*) as total
		FROM patients
		WHERE age > ?
		GROUP BY status
	`
	err = patientRepo.RawQuery(context.Background(), sqlQuery, &rawResults, 30)
	if err != nil {
		panic(err)
	}

	for _, res := range rawResults {
		fmt.Printf("  Status: %-12s | Total: %d\n", res.Status, res.Total)
	}
}

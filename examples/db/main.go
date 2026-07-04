package main

import (
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
	// cfg := orm.DefaultConfig()
	// cfg.Host = "localhost"
	// cfg.DBName = "his_db"
	// dbConn, err := orm.New(cfg)

	// For this example, we'll use an in-memory SQLite connection directly
	dbConn, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		panic("failed to connect to db")
	}

	// 2. Auto-migrate schema
	dbConn.AutoMigrate(&Patient{})

	// 3. Seed some dummy data
	dbConn.Create([]Patient{
		{FullName: "Nguyen Van A", Age: 45, Status: "WAITING"},
		{FullName: "Tran Thi B", Age: 32, Status: "IN_PROGRESS"},
		{FullName: "Le Van C", Age: 28, Status: "WAITING"},
		{FullName: "Pham Thi D", Age: 65, Status: "COMPLETED"},
		{FullName: "Hoang Van E", Age: 50, Status: "WAITING"},
	})

	// 4. Perform a Paginated Query for "WAITING" patients
	fmt.Println("\n--- Fetching WAITING Patients (Page 1, Size 2) ---")
	
	// Create standard PageRequest (often populated from Gin/Fiber context binding)
	req := orm.PageRequest{
		Page:  1,
		Size:  2,
		Sorts: []string{"age desc"},
	}

	// Build the GORM query with business logic
	query := dbConn.Model(&Patient{}).Where("status = ?", "WAITING")

	// Execute pagination
	resp, err := orm.ExecutePagination[Patient](query, req)
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
}

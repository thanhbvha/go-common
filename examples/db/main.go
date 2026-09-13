package main

import (
	"context"
	"fmt"

	"github.com/glebarez/sqlite"
	"github.com/thanhbvha/go-common/db/orm"
	"github.com/thanhbvha/go-common/telemetry"
	"gorm.io/gorm"
	"gorm.io/plugin/opentelemetry/tracing"
)

// ---------------------------------------------------------
// Models
// ---------------------------------------------------------
type Patient struct {
	gorm.Model
	FullName string
	Age      int
	Status   string
}

type Department struct {
	gorm.Model
	Name string
}

type Doctor struct {
	gorm.Model
	Name         string
	DepartmentID uint
}

type Visit struct {
	gorm.Model
	PatientID uint
	DoctorID  uint
	Diagnosis string
}

// ---------------------------------------------------------
// Main Menu
// ---------------------------------------------------------
func main() {
	fmt.Println("=== Database/ORM Module Examples ===")
	dbConn := setupDatabaseAndSeed()

	// Uncomment the example you want to run:
	
	RunMultiDatabaseConfigExample()
	RunPaginationExample(dbConn)
	// RunAggregateAndRawQueryExample(dbConn)
	// RunTransactionExample(dbConn)
	// RunComplexJoinExample(dbConn)
}

// =====================================================================
// 0. Multi-Database Configuration & Connection Example
// =====================================================================
func RunMultiDatabaseConfigExample() {
	fmt.Println("\n--- 0. Multi-Database Configuration Example ---")
	
	// Create configuration for two separate PostgreSQL databases
	configs := map[string]orm.Config{
		"primary_db": {
			Host:            "localhost",
			Port:            5432,
			User:            "postgres",
			Password:        "password",
			DBName:          "app_main",
			MaxOpenConns:    100,
			MaxIdleConns:    10,
			EnableTelemetry: true,
		},
		"analytics_db": {
			Host:            "localhost",
			Port:            5432,
			User:            "postgres",
			Password:        "password",
			DBName:          "app_analytics",
			MaxOpenConns:    50,
			MaxIdleConns:    5,
			EnableTelemetry: false,
		},
	}

	// Initialize the global ORM Manager. 
	// The second parameter specifies the default database.
	// NOTE: In a real environment without these DBs running, this will return an error.
	err := orm.Init(configs, "primary_db")
	if err != nil {
		fmt.Printf("Notice: Failed to connect to PostgreSQL (expected in this demo without a real DB running): %v\n", err)
	} else {
		// CRITICAL: Always close connections on application shutdown
		defer orm.Close()
		fmt.Println("Successfully connected to multiple databases!")
	}

	// Usage anywhere in your application:
	// mainDB := orm.Get()               // Returns the default "primary_db"
	// analyticsDB := orm.Get("analytics_db") // Returns the specific DB

	fmt.Println("Configuration pattern demonstrated successfully.")
}

// =====================================================================
// 1. Pagination Example (Using Generic Repository)
// =====================================================================
func RunPaginationExample(dbConn *gorm.DB) {
	fmt.Println("\n--- 1. Fetching WAITING Patients (Page 1, Size 2) ---")
	patientRepo := orm.NewRepository[Patient](dbConn)

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

	fmt.Printf("Total Records: %d\n", resp.TotalRows)
	fmt.Printf("Total Pages: %d\n", resp.TotalPages)
	fmt.Printf("Current Page: %d\n", resp.Page)

	for i, p := range resp.Items {
		fmt.Printf("  %d. %s (Age: %d)\n", i+1, p.FullName, p.Age)
	}
}

// =====================================================================
// 2. Aggregate & Raw SQL Example
// =====================================================================
func RunAggregateAndRawQueryExample(dbConn *gorm.DB) {
	patientRepo := orm.NewRepository[Patient](dbConn)

	type StatusCount struct {
		Status string
		Total  int
	}

	// Example A: Using Aggregate (Query Builder)
	fmt.Println("\n--- 2A. Aggregate: Group by Status ---")
	var aggResults []StatusCount
	err := patientRepo.Aggregate(context.Background(), &aggResults, func(db *gorm.DB) *gorm.DB {
		return db.Select("status, count(*) as total").Group("status")
	})
	if err != nil {
		panic(err)
	}
	for _, res := range aggResults {
		fmt.Printf("  Status: %-12s | Total: %d\n", res.Status, res.Total)
	}

	// Example B: Using RawQuery (Manual SQL)
	fmt.Println("\n--- 2B. RawQuery: Custom SQL (Age > 30) ---")
	var rawResults []StatusCount
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

// =====================================================================
// 3. Transaction Example (Using WithTx)
// =====================================================================
func RunTransactionExample(dbConn *gorm.DB) {
	fmt.Println("\n--- 3. Transaction: Insert with WithTx ---")
	patientRepo := orm.NewRepository[Patient](dbConn)

	err := dbConn.Transaction(func(tx *gorm.DB) error {
		// CRITICAL: Clone repo with Transaction DB using WithTx()
		txRepo := patientRepo.WithTx(tx)

		err := txRepo.Insert(context.Background(), &Patient{
			FullName: "Tx Patient",
			Age:      20,
			Status:   "WAITING",
		})
		if err != nil {
			fmt.Println("  [Error] Insert failed, rolling back...")
			return err
		}
		
		fmt.Println("  [Success] Inserted Tx Patient within transaction.")
		return nil // Returning nil will automatically Commit
	})
	
	if err != nil {
		panic(err)
	}
}

// =====================================================================
// 4. Complex JOIN Example
// =====================================================================
func RunComplexJoinExample(dbConn *gorm.DB) {
	fmt.Println("\n--- 4. Aggregate: Complex JOIN (Patient -> Visit -> Doctor -> Department) ---")
	patientRepo := orm.NewRepository[Patient](dbConn)

	type VisitDetail struct {
		PatientName    string
		Diagnosis      string
		DoctorName     string
		DepartmentName string
	}
	var visitDetails []VisitDetail

	// Query to get list of patients visiting Cardiology department
	err := patientRepo.Aggregate(context.Background(), &visitDetails, func(db *gorm.DB) *gorm.DB {
		return db.Select("patients.full_name as patient_name, visits.diagnosis, doctors.name as doctor_name, departments.name as department_name").
			Joins("JOIN visits ON visits.patient_id = patients.id").
			Joins("JOIN doctors ON doctors.id = visits.doctor_id").
			Joins("JOIN departments ON departments.id = doctors.department_id").
			Where("departments.name = ?", "Cardiology")
	})
	if err != nil {
		panic(err)
	}

	for _, vd := range visitDetails {
		fmt.Printf("  Patient: %-15s | Diagnosis: %-20s | Doctor: %-15s | Dept: %s\n", vd.PatientName, vd.Diagnosis, vd.DoctorName, vd.DepartmentName)
	}
}

// ---------------------------------------------------------
// Helper: Setup DB & Seed Data
// ---------------------------------------------------------
func setupDatabaseAndSeed() *gorm.DB {
	// Initialize Telemetry
	tel, err := telemetry.Init(context.Background(), telemetry.Config{
		ServiceName:   "demo-db-service",
		EnableTracing: true,
		Endpoint:      "localhost:4317",
	})
	if err == nil {
		// In a real app, defer this in main()
		defer tel.Shutdown(context.Background())
	}

	// 1. Initialize In-Memory SQLite connection
	dbConn, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		panic("failed to connect to db")
	}

	// 2. Enable Telemetry Plugin for GORM
	if err := dbConn.Use(tracing.NewPlugin()); err != nil {
		fmt.Printf("Warning: failed to enable tracing: %v\n", err)
	}

	// 3. Auto-migrate schema
	dbConn.AutoMigrate(&Patient{}, &Department{}, &Doctor{}, &Visit{})

	// 4. Seed Data
	patientRepo := orm.NewRepository[Patient](dbConn)
	_ = patientRepo.InsertMany(context.Background(), []Patient{
		{FullName: "Nguyen Van A", Age: 45, Status: "WAITING"},
		{FullName: "Tran Thi B", Age: 32, Status: "IN_PROGRESS"},
		{FullName: "Le Van C", Age: 28, Status: "WAITING"},
		{FullName: "Pham Thi D", Age: 65, Status: "COMPLETED"},
		{FullName: "Hoang Van E", Age: 50, Status: "WAITING"},
	})

	deptRepo := orm.NewRepository[Department](dbConn)
	_ = deptRepo.InsertMany(context.Background(), []Department{{Name: "Cardiology"}, {Name: "Neurology"}})

	doctorRepo := orm.NewRepository[Doctor](dbConn)
	_ = doctorRepo.InsertMany(context.Background(), []Doctor{
		{Name: "Dr. Smith", DepartmentID: 1},
		{Name: "Dr. Strange", DepartmentID: 2},
	})

	visitRepo := orm.NewRepository[Visit](dbConn)
	_ = visitRepo.InsertMany(context.Background(), []Visit{
		{PatientID: 1, DoctorID: 1, Diagnosis: "Heart burn"},
		{PatientID: 2, DoctorID: 2, Diagnosis: "Headache"},
		{PatientID: 3, DoctorID: 1, Diagnosis: "High blood pressure"},
	})

	return dbConn
}

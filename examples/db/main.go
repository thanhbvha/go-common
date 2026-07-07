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

func main() {
	fmt.Println("--- Database/ORM Example ---")

	// 0. Initialize Telemetry (Enable Tracing)
	tel, err := telemetry.Init(context.Background(), telemetry.Config{
		ServiceName:   "demo-db-service",
		EnableTracing: true,
		Endpoint:      "localhost:4317",
	})
	if err == nil {
		defer tel.Shutdown(context.Background())
	}

	// 1. In a real app, you would load this from config using the 'config' module
	// For this example, we'll use an in-memory SQLite connection directly
	dbConn, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		panic("failed to connect to db")
	}

	// Enable Telemetry Plugin for GORM
	if err := dbConn.Use(tracing.NewPlugin()); err != nil {
		fmt.Printf("Warning: failed to enable tracing: %v\n", err)
	}

	// 2. Auto-migrate schema
	dbConn.AutoMigrate(&Patient{}, &Department{}, &Doctor{}, &Visit{})

	// Initialize Repository first
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

	// Seed relations using Repositories
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
	// STATISTICS EXAMPLES
	// ==========================================

	type StatusCount struct {
		Status string
		Total  int
	}

	// 5. Example 1: Using Aggregate (Query Builder)
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

	// 6. Example 2: Using RawQuery (Manual SQL)
	fmt.Println("\n--- 2. RawQuery: Custom SQL (Age > 30) ---")
	var rawResults []StatusCount

	// Note: Table name must be hardcoded ("patients") because we are using raw SQL
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

	// ==========================================
	// TRANSACTION EXAMPLES
	// ==========================================

	// 7. Example 3: Using Transaction
	fmt.Println("\n--- 3. Transaction: Insert with WithTx ---")
	err = dbConn.Transaction(func(tx *gorm.DB) error {
		// Clone repo with Transaction DB
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

	// 8. Example 4: Complex JOIN (3-4 tables) using Aggregate
	fmt.Println("\n--- 4. Aggregate: Complex JOIN (Patient -> Visit -> Doctor -> Department) ---")
	type VisitDetail struct {
		PatientName    string
		Diagnosis      string
		DoctorName     string
		DepartmentName string
	}
	var visitDetails []VisitDetail

	// Query to get list of patients visiting Cardiology department
	err = patientRepo.Aggregate(context.Background(), &visitDetails, func(db *gorm.DB) *gorm.DB {
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

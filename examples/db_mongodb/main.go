package main

import (
	"context"
	"fmt"
	"log"

	"github.com/thanhbvha/go-common/db/mongodb"
	"github.com/thanhbvha/go-common/telemetry"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type Patient struct {
	ID       bson.ObjectID `bson:"_id,omitempty"`
	FullName string        `bson:"full_name"`
	Age      int           `bson:"age"`
	Status   string        `bson:"status"`
}

func main() {
	fmt.Println("--- MongoDB Example ---")
	ctx := context.Background()

	// 0. Khởi tạo Telemetry (Bật Tracing)
	tel, err := telemetry.Init(ctx, telemetry.Config{
		ServiceName:   "demo-mongodb-service",
		EnableTracing: true,
		Endpoint:      "localhost:4317",
	})
	if err == nil {
		defer tel.Shutdown(ctx)
	}

	// 1. Config for Multiple Databases
	cfgPrimary := mongodb.DefaultConfig()
	cfgPrimary.URI = "mongodb://localhost:27017"
	cfgPrimary.DBName = "primary_db"
	cfgPrimary.PingTimeout = 2 * 1000 * 1000 * 1000
	cfgPrimary.EnableTelemetry = true // Bật Telemetry cho kết nối này

	cfgLog := mongodb.DefaultConfig()
	cfgLog.URI = "mongodb://localhost:27017"
	cfgLog.DBName = "log_db"
	cfgLog.PingTimeout = 2 * 1000 * 1000 * 1000
	cfgLog.EnableTelemetry = true // Bật Telemetry cho kết nối này

	configs := map[string]mongodb.Config{
		"primary": cfgPrimary,
		"logger":  cfgLog,
	}

	// 2. Initialize the Global Manager
	// We can pass "primary" as the default. 
	// (If we had only 1 config, we wouldn't even need to pass it).
	err = mongodb.Init(ctx, configs, "primary")
	if err != nil {
		log.Printf("Failed to connect to MongoDB (make sure it is running locally): %v\n", err)
		return
	}
	defer mongodb.DisconnectAll(ctx)

	fmt.Println("Connected to multiple MongoDB instances successfully!")

	// 3. Usage Anywhere in the Application
	
	// Get the default DB and create a Generic Repository for 'Patient'
	patientRepo := mongodb.NewRepository[Patient](mongodb.Get(), "patients")
	
	// We can still get a specific DB for other repos
	logDB := mongodb.Get("logger")
	auditColl := logDB.Collection("audit_logs")

	fmt.Printf("Default DB Name: %s\n", mongodb.Get().Name())
	fmt.Printf("Logger DB Name: %s\n", logDB.Name())

	// 4. Perform Paginated Query for "WAITING" patients
	fmt.Println("\n--- Fetching WAITING Patients (Page 1, Size 2) ---")
	req := mongodb.PageRequest{
		Page: 1,
		Size: 2,
	}

	filter := bson.M{"status": "WAITING"}

	// Use pagination on the generic repository (much cleaner!)
	resp, err := patientRepo.Paginate(ctx, filter, req)
	if err != nil {
		log.Fatalf("Pagination failed: %v", err)
	}

	fmt.Printf("Total Records: %d\n", resp.TotalRows)
	fmt.Printf("Total Pages: %d\n", resp.TotalPages)
	fmt.Printf("Current Page: %d\n", resp.Page)
	
	for i, p := range resp.Items {
		fmt.Printf("  %d. %s (Age: %d)\n", i+1, p.FullName, p.Age)
	}
	
	// Example of using other repo functions
	exists, _ := patientRepo.Exists(ctx, filter)
	fmt.Printf("Does WAITING patient exist? %v\n", exists)
	
	// Just a mock check to use auditColl and avoid declared-and-not-used error
	_ = auditColl
}

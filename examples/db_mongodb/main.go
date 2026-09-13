package main

import (
	"context"
	"fmt"
	"log"

	"github.com/thanhbvha/go-common/db/mongodb"
	"github.com/thanhbvha/go-common/telemetry"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ---------------------------------------------------------
// Models
// ---------------------------------------------------------
type Patient struct {
	ID       bson.ObjectID `bson:"_id,omitempty"`
	FullName string        `bson:"full_name"`
	Age      int           `bson:"age"`
	Status   string        `bson:"status"`
}

// ---------------------------------------------------------
// Main Menu
// ---------------------------------------------------------
func main() {
	fmt.Println("=== MongoDB Module Examples ===")
	
	ctx := context.Background()
	setupMongoDB(ctx)
	
	// CRITICAL: Always DisconnectAll after usage to flush background connections
	defer mongodb.DisconnectAll(ctx)

	// Uncomment the example you want to run:
	RunMongoPaginationExample(ctx)
	// RunMongoMultipleDatabasesExample(ctx)
}

// =====================================================================
// 1. Pagination & Generic Repository Example
// =====================================================================
func RunMongoPaginationExample(ctx context.Context) {
	fmt.Println("\n--- 1. Fetching WAITING Patients (Page 1, Size 2) ---")
	
	// Get the default DB and create a Generic Repository for 'Patient'
	patientRepo := mongodb.NewRepository[Patient](mongodb.Get(), "patients")

	req := mongodb.PageRequest{
		Page: 1,
		Size: 2,
	}
	filter := bson.M{"status": "WAITING"}

	// Use pagination on the generic repository
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
	
	// Check Existence
	exists, _ := patientRepo.Exists(ctx, filter)
	fmt.Printf("Does WAITING patient exist? %v\n", exists)
}

// =====================================================================
// 2. Multiple Databases Example
// =====================================================================
func RunMongoMultipleDatabasesExample(ctx context.Context) {
	fmt.Println("\n--- 2. Accessing Multiple Databases ---")

	// The default DB
	defaultDB := mongodb.Get()
	fmt.Printf("Default DB Name: %s\n", defaultDB.Name())

	// The 'logger' DB (configured in setup)
	logDB := mongodb.Get("logger")
	fmt.Printf("Logger DB Name: %s\n", logDB.Name())

	// Native Collection access if you don't want to use Generic Repository
	auditColl := logDB.Collection("audit_logs")
	fmt.Printf("Audit Collection initialized: %s\n", auditColl.Name())
}

// ---------------------------------------------------------
// Helper: Setup MongoDB Connections
// ---------------------------------------------------------
func setupMongoDB(ctx context.Context) {
	// Initialize Telemetry
	tel, err := telemetry.Init(ctx, telemetry.Config{
		ServiceName:   "demo-mongodb-service",
		EnableTracing: true,
		Endpoint:      "localhost:4317",
	})
	if err == nil {
		defer tel.Shutdown(ctx) // In real app, put in main
	}

	// 1. Config for Multiple Databases
	cfgPrimary := mongodb.DefaultConfig()
	cfgPrimary.URI = "mongodb://localhost:27017"
	cfgPrimary.DBName = "primary_db"
	cfgPrimary.PingTimeout = 2 * 1000 * 1000 * 1000
	cfgPrimary.EnableTelemetry = true

	cfgLog := mongodb.DefaultConfig()
	cfgLog.URI = "mongodb://localhost:27017"
	cfgLog.DBName = "log_db"
	cfgLog.PingTimeout = 2 * 1000 * 1000 * 1000
	cfgLog.EnableTelemetry = true

	configs := map[string]mongodb.Config{
		"primary": cfgPrimary,
		"logger":  cfgLog,
	}

	// 2. Initialize the Global Manager
	err = mongodb.Init(ctx, configs, "primary")
	if err != nil {
		log.Printf("Failed to connect to MongoDB (make sure it is running locally): %v\n", err)
		return
	}
	fmt.Println("[Setup] Connected to multiple MongoDB instances successfully!")
}

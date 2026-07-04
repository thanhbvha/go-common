package main

import (
	"context"
	"fmt"
	"log"

	"github.com/thanhbvha/go-common/db/mongodb"
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

	// 1. Config
	cfg := mongodb.DefaultConfig()
	// You can replace it with a valid URI like: mongodb://localhost:27017
	cfg.URI = "mongodb://localhost:27017"
	cfg.PingTimeout = 2 * 1000 * 1000 * 1000 // 2 seconds

	// 2. Connect
	client, err := mongodb.New(ctx, cfg)
	if err != nil {
		log.Printf("Failed to connect to MongoDB (make sure it is running locally): %v\n", err)
		return
	}
	defer client.Disconnect(ctx)

	fmt.Println("Connected to MongoDB successfully!")

	// 3. Select Database and Collection
	db := mongodb.GetDatabase(client, "his_db")
	patientsColl := db.Collection("patients")

	// 4. Perform Paginated Query for "WAITING" patients
	fmt.Println("\n--- Fetching WAITING Patients (Page 1, Size 2) ---")
	req := mongodb.PageRequest{
		Page: 1,
		Size: 2,
	}

	filter := bson.M{"status": "WAITING"}

	resp, err := mongodb.ExecutePagination[Patient](ctx, patientsColl, filter, req)
	if err != nil {
		log.Fatalf("Pagination failed: %v", err)
	}

	fmt.Printf("Total Records: %d\n", resp.TotalRows)
	fmt.Printf("Total Pages: %d\n", resp.TotalPages)
	fmt.Printf("Current Page: %d\n", resp.Page)
	
	for i, p := range resp.Items {
		fmt.Printf("  %d. %s (Age: %d)\n", i+1, p.FullName, p.Age)
	}
}

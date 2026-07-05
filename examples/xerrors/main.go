package main

import (
	goErrors "errors"
	"fmt"
	"log"

	"github.com/thanhbvha/go-common/xerrors"
)

func main() {
	// 1. Using a standard predefined error
	err := processRequest(false)
	if err != nil {
		fmt.Println("--- Standard Error Handling ---")
		logError(err)
	}

	// 2. Wrapping a system/3rd-party error
	err = fetchFromDB()
	if err != nil {
		fmt.Println("\n--- Wrapped Error Handling ---")
		logError(err)
	}
}

// processRequest simulates a request that fails authentication
func processRequest(valid bool) error {
	if !valid {
		// Return a predefined error from the xerrors package
		return xerrors.ErrUnauthorized
	}
	return nil
}

// fetchFromDB simulates a DB error wrapped with business context
func fetchFromDB() error {
	// Simulate an underlying driver error
	dbErr := goErrors.New("connection reset by peer")

	// Wrap it with our standard error
	return xerrors.Wrap(dbErr, "DB_CONNECTION_FAILED", "Could not connect to the database", xerrors.StatusInternalServerError)
}

// logError is a helper to simulate how an HTTP framework (like Fiber or Gin)
// would handle and log the error.
func logError(err error) {
	// Extract the HTTP status code to send to the client
	httpStatus := xerrors.HTTPStatusCode(err)
	
	// Extract the custom code
	code := xerrors.GetCode(err)

	fmt.Printf("HTTP Status sent to client: %d\n", httpStatus)
	fmt.Printf("Error Code sent to client: %s\n", code)

	// Print the full error (which includes the wrapped cause for our internal logs)
	log.Printf("Internal System Log: %v\n", err)
	
	// Demonstrate xerrors.Is
	if xerrors.Is(err, xerrors.ErrUnauthorized) {
		fmt.Println("-> This was an unauthorized error!")
	}
}

package main

import (
	goErrors "errors"
	"fmt"
	"log"

	"github.com/thanhbvha/go-common/xerrors"
)

func main() {
	fmt.Println("=== XErrors Module Examples ===")

	RunStandardErrorExample()
	RunWrappedErrorExample()
	RunErrorCheckingExample()
	RunErrorJoinExample()
}

// =====================================================================
// 1. Standard Predefined Error Handling
// =====================================================================
func RunStandardErrorExample() {
	fmt.Println("\n--- 1. Standard Error Handling ---")
	
	// Simulate an authentication failure
	err := xerrors.ErrUnauthorized

	logError(err)
}

// =====================================================================
// 2. Wrapping a System/3rd-party Error
// =====================================================================
func RunWrappedErrorExample() {
	fmt.Println("\n--- 2. Wrapped Error Handling ---")
	
	// Simulate an underlying driver error (e.g., from Postgres or Redis)
	dbErr := goErrors.New("connection reset by peer: timeout 30s")

	// CRITICAL: Always use xerrors.Wrap() for system/3rd-party errors.
	// This preserves the original error stack trace for debugging while returning
	// a safe, sanitized "DB_CONNECTION_FAILED" message to the external API client.
	err := xerrors.Wrap(dbErr, "DB_CONNECTION_FAILED", "Could not connect to the database", xerrors.StatusInternalServerError)

	logError(err)
}

// =====================================================================
// 3. Error Type Checking (Is / As)
// =====================================================================
func RunErrorCheckingExample() {
	fmt.Println("\n--- 3. Error Type Checking (Is / As) ---")

	// Simulate returning a NOT FOUND error
	err := xerrors.ErrNotFound

	// Use xerrors.Is to check if it matches a specific error
	if xerrors.Is(err, xerrors.ErrNotFound) {
		fmt.Println("Check successful: The error is a NotFound error!")
	} else {
		fmt.Println("Check failed: The error is NOT a NotFound error!")
	}
}

// =====================================================================
// 4. Joining Multiple Errors (Go 1.20+)
// =====================================================================
func RunErrorJoinExample() {
	fmt.Println("\n--- 4. Joining Multiple Errors ---")

	// Simulate validating a form where multiple fields are invalid
	err1 := xerrors.New("INVALID_EMAIL", "The email format is incorrect", xerrors.StatusBadRequest)
	err2 := xerrors.New("PASSWORD_TOO_SHORT", "The password must be at least 8 characters", xerrors.StatusBadRequest)

	// Join them together!
	joinedErr := xerrors.Join(err1, err2)

	fmt.Printf("Joined Error Output:\n%v\n", joinedErr)
}

// logError is a helper to simulate how an HTTP framework (like Fiber or Gin)
// would handle and log the error.
func logError(err error) {
	// Extract the HTTP status code to send to the client (e.g. 401, 500)
	httpStatus := xerrors.HTTPStatusCode(err)
	
	// Extract the custom machine-readable code (e.g. "UNAUTHORIZED", "DB_CONNECTION_FAILED")
	code := xerrors.GetCode(err)

	fmt.Printf("HTTP Status sent to client: %d\n", httpStatus)
	fmt.Printf("Error Code sent to client: %s\n", code)

	// Print the full error (which includes the wrapped cause for our internal logs)
	log.Printf("Internal System Log: %v\n", err)
}

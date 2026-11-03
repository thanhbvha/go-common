package health

import (
	"context"
	"fmt"
	"net/http"
)

// HTTPChecker returns a CheckFunc that performs a GET to the given URL.
// expectedStatus is the HTTP status code considered healthy (e.g., 200).
func HTTPChecker(url string, expectedStatus int) CheckFunc {
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != expectedStatus {
			return fmt.Errorf("unexpected status: got %d, want %d", resp.StatusCode, expectedStatus)
		}
		return nil
	}
}

// CustomChecker wraps any function as a CheckFunc for one-off checks.
func CustomChecker(fn func(ctx context.Context) error) CheckFunc {
	return fn
}

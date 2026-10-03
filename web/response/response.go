// Package response provides a standardized JSON response structure for API endpoints.
//
// It ensures that all HTTP responses follow a consistent format across the application,
// making it easier for client-side applications to parse successful data and errors.
//
// Basic usage:
//
//	func GetUser(c *fiber.Ctx) error {
//		// ...
//		return response.Success(c, user)
//	}
package response

// Response represents the standard API response structure
type Response struct {
	Code      int         `json:"code"`
	Message   string      `json:"message"`
	Data      interface{} `json:"data,omitempty"`
	Errors    interface{} `json:"errors,omitempty"`
	RequestID string      `json:"request_id,omitempty"`
}

// PaginatedResponse is the standard structure for paginated list API responses.
type PaginatedResponse[T any] struct {
	Code       int    `json:"code"`
	Message    string `json:"message"`
	Data       []T    `json:"data"`
	TotalRows  int64  `json:"total_rows"`
	TotalPages int    `json:"total_pages"`
	Page       int    `json:"page"`
	Size       int    `json:"size"`
	RequestID  string `json:"request_id,omitempty"`
}

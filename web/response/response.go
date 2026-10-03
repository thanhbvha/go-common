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

import (
	"github.com/gofiber/fiber/v2"
)

// Response represents the standard API response structure
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data      interface{} `json:"data,omitempty"`
	Errors    interface{} `json:"errors,omitempty"`
	RequestID string      `json:"request_id,omitempty"`
}

// getRequestID safely extracts the request ID from Fiber's context
func getRequestID(c *fiber.Ctx) string {
	if reqID, ok := c.Locals("request_id").(string); ok {
		return reqID
	}
	return ""
}

// Success returns a standard success response
func Success(c *fiber.Ctx, data interface{}) error {
	return c.Status(fiber.StatusOK).JSON(Response{
		Code:      0,
		Message:   "Success",
		Data:      data,
		RequestID: getRequestID(c),
	})
}

// Error returns a standard error response
func Error(c *fiber.Ctx, statusCode int, message string) error {
	return c.Status(statusCode).JSON(Response{
		Code:      statusCode,
		Message:   message,
		RequestID: getRequestID(c),
	})
}

// ValidationError returns a standard validation error response
func ValidationError(c *fiber.Ctx, errors interface{}) error {
	return c.Status(fiber.StatusBadRequest).JSON(Response{
		Code:      fiber.StatusBadRequest,
		Message:   "Validation failed",
		Errors:    errors,
		RequestID: getRequestID(c),
	})
}

// Created returns a 201 Created response
func Created(c *fiber.Ctx, data interface{}) error {
	return c.Status(fiber.StatusCreated).JSON(Response{
		Code:      0,
		Message:   "Created",
		Data:      data,
		RequestID: getRequestID(c),
	})
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

// Paginated returns a standard paginated list response for Fiber.
//
// Example:
//
//	page, _ := repo.Paginate(ctx, orm.PageRequest{Page: 1, Size: 20})
//	return response.Paginated(c, page.Items, page.TotalRows, page.TotalPages, page.Page, page.Size)
func Paginated[T any](c *fiber.Ctx, items []T, totalRows int64, totalPages, page, size int) error {
	return c.Status(fiber.StatusOK).JSON(PaginatedResponse[T]{
		Code:       0,
		Message:    "Success",
		Data:       items,
		TotalRows:  totalRows,
		TotalPages: totalPages,
		Page:       page,
		Size:       size,
		RequestID:  getRequestID(c),
	})
}

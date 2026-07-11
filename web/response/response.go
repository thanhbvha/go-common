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

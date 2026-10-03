package fiberresp

import (
	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/web/response"
)

// getRequestID safely extracts the request ID from Fiber's context
func getRequestID(c *fiber.Ctx) string {
	if reqID, ok := c.Locals("request_id").(string); ok {
		return reqID
	}
	return ""
}

// Success returns a standard success response
func Success(c *fiber.Ctx, data interface{}) error {
	return c.Status(fiber.StatusOK).JSON(response.Response{
		Code:      0,
		Message:   "Success",
		Data:      data,
		RequestID: getRequestID(c),
	})
}

// Error returns a standard error response
func Error(c *fiber.Ctx, statusCode int, message string) error {
	return c.Status(statusCode).JSON(response.Response{
		Code:      statusCode,
		Message:   message,
		RequestID: getRequestID(c),
	})
}

// ValidationError returns a standard validation error response
func ValidationError(c *fiber.Ctx, errors interface{}) error {
	return c.Status(fiber.StatusBadRequest).JSON(response.Response{
		Code:      fiber.StatusBadRequest,
		Message:   "Validation failed",
		Errors:    errors,
		RequestID: getRequestID(c),
	})
}

// Created returns a 201 Created response
func Created(c *fiber.Ctx, data interface{}) error {
	return c.Status(fiber.StatusCreated).JSON(response.Response{
		Code:      0,
		Message:   "Created",
		Data:      data,
		RequestID: getRequestID(c),
	})
}

// Paginated returns a standard paginated list response for Fiber.
func Paginated[T any](c *fiber.Ctx, items []T, totalRows int64, totalPages, page, size int) error {
	return c.Status(fiber.StatusOK).JSON(response.PaginatedResponse[T]{
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

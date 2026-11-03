// Package fiberresp writes standardized API responses for Fiber.
package fiberresp

import (
	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/v2/utils/ctxkey"
	"github.com/thanhbvha/go-common/v2/web/response"
)

// getRequestID safely extracts the request ID from Fiber's context
func getRequestID(c *fiber.Ctx) string {
	if reqID, ok := c.Locals("request_id").(string); ok {
		return reqID
	}
	if reqID, ok := ctxkey.GetRequestID(c.UserContext()); ok {
		return reqID
	}
	return ""
}

// Success returns a standard success response
func Success(c *fiber.Ctx, data interface{}) error {
	return c.Status(fiber.StatusOK).JSON(response.NewSuccess(data, getRequestID(c)))
}

// Error returns a standard error response
func Error(c *fiber.Ctx, statusCode int, message string) error {
	payload := response.NewError(statusCode, message, getRequestID(c))
	return c.Status(payload.Code).JSON(payload)
}

// ValidationError returns a standard validation error response
func ValidationError(c *fiber.Ctx, errors interface{}) error {
	return c.Status(fiber.StatusBadRequest).JSON(response.NewValidationError(errors, getRequestID(c)))
}

// Created returns a 201 Created response
func Created(c *fiber.Ctx, data interface{}) error {
	return c.Status(fiber.StatusCreated).JSON(response.NewCreated(data, getRequestID(c)))
}

// Paginated returns a standard paginated list response for Fiber.
func Paginated[T any](c *fiber.Ctx, items []T, totalRows int64, totalPages, page, size int) error {
	return c.Status(fiber.StatusOK).JSON(response.NewPaginated(items, totalRows, totalPages, page, size, getRequestID(c)))
}

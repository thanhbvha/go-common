package response

import (
	"github.com/gofiber/fiber/v2"
)

// Response represents the standard API response structure
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
	Errors  interface{} `json:"errors,omitempty"`
}

// Success returns a standard success response
func Success(c *fiber.Ctx, data interface{}) error {
	return c.Status(fiber.StatusOK).JSON(Response{
		Code:    0,
		Message: "Success",
		Data:    data,
	})
}

// Error returns a standard error response
func Error(c *fiber.Ctx, statusCode int, message string) error {
	return c.Status(statusCode).JSON(Response{
		Code:    statusCode,
		Message: message,
	})
}

// ValidationError returns a standard validation error response
func ValidationError(c *fiber.Ctx, errors interface{}) error {
	return c.Status(fiber.StatusBadRequest).JSON(Response{
		Code:    fiber.StatusBadRequest,
		Message: "Validation failed",
		Errors:  errors,
	})
}

// Created returns a 201 Created response
func Created(c *fiber.Ctx, data interface{}) error {
	return c.Status(fiber.StatusCreated).JSON(Response{
		Code:    0,
		Message: "Created",
		Data:    data,
	})
}

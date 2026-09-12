# XErrors Module

## Overview
The `xerrors` module provides a standardized error wrapping and categorization system. It bridges the gap between Go's standard `error` interface and structured HTTP APIs, allowing you to return rich error details to logs while exposing safe, sanitized status codes to clients.

## Key Features
- `xerrors.New(code, message, httpStatus)`: Creates a new business error.
- `xerrors.Wrap(err, code, message, httpStatus)`: Wraps an existing (e.g., driver) error.
- `xerrors.HTTPStatusCode(err)`: Automatically extracts the correct HTTP status code for Fiber/Gin responses.
- `xerrors.GetCode(err)`: Extracts the machine-readable error string (e.g., `USER_NOT_FOUND`).

## 🚨 Best Practices for AI/Developers
- **Stop using `errors.New`**: Never use standard `errors.New` or `fmt.Errorf` in business logic layers. Always use `xerrors.New` or `xerrors.Wrap`.
- **System Errors (CRITICAL)**: When a database driver, Redis, or HTTP client returns an error, NEVER return it directly to the user. Always use `xerrors.Wrap(err, "SYSTEM_ERROR", "A secure message", 500)`. This ensures that sensitive SQL syntax or internal IP addresses are logged internally but stripped from the JSON response sent to the client.

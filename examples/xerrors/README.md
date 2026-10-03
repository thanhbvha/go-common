# XErrors Module

## Overview
The `xerrors` module provides a standardized error wrapping and categorization system. It bridges the gap between Go's standard `error` interface and structured HTTP APIs, allowing you to return rich error details to logs while exposing safe, sanitized status codes to clients.

## Key Features
- `xerrors.New(code, message, httpStatus)`: Creates a new business error with a captured stack trace.
- `xerrors.NewWithFields(code, message, httpStatus, fields)`: Creates a new error with structured context fields and a stack trace.
- `xerrors.Wrap(err, code, message, httpStatus)`: Wraps an existing (e.g., driver) error, capturing a new stack trace.
- `xerrors.HTTPStatusCode(err)`: Automatically extracts the correct HTTP status code for Fiber/Gin responses.
- `xerrors.GetCode(err)`: Extracts the machine-readable error string (e.g., `USER_NOT_FOUND`).
- `xerrors.Is(err, target)`: Convenience wrapper to check if an error matches a specific target.
- `xerrors.Join(errs...)`: Combines multiple errors into one (utilizing Go 1.20+ `errors.Join`).
- `xerrors.StackTraceString(err)`: Formats the captured stack trace into a readable string.
- `xerrors.GetFields(err)`: Extracts structured context fields from the error.

## Covered Examples
1. `RunStandardErrorExample()`: Demonstrates how to use and log predefined standard errors.
2. `RunWrappedErrorExample()`: Demonstrates how to wrap system/3rd-party errors to prevent sensitive data leaks.
3. `RunErrorCheckingExample()`: Demonstrates how to check error types using `xerrors.Is()`.
4. `RunErrorJoinExample()`: Demonstrates how to combine multiple errors together, which is incredibly useful for form validation.
5. `RunStackTraceAndFieldsExample()`: Demonstrates how to attach structured context fields to an error and print its full stack trace.

## 🚨 Best Practices for AI/Developers
- **Stop using `errors.New`**: Never use standard `errors.New` or `fmt.Errorf` in business logic layers. Always use `xerrors.New` or `xerrors.Wrap`.
- **System Errors (CRITICAL)**: When a database driver, Redis, or HTTP client returns an error, NEVER return it directly to the user. Always use `xerrors.Wrap(err, "SYSTEM_ERROR", "A secure message", 500)`. This ensures that sensitive SQL syntax or internal IP addresses are logged internally but stripped from the JSON response sent to the client.

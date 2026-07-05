# xerrors

A structured error handling package that wraps standard Go errors with business-level contexts, such as String Codes (e.g., `UNAUTHORIZED`) and HTTP Status Codes. This makes it trivial to map backend errors directly to REST API responses.

### Key Features
- **Structured Errors:** Every error carries a `Code`, `Message`, and `HTTPStatus`.
- **Error Wrapping:** Chain errors together using `xerrors.Wrap` while preserving the root cause.
- **Pre-defined Common Errors:** Ready-to-use variables like `ErrNotFound`, `ErrUnauthorized`, and `ErrInternal`.
- **Standard Library Compatibility:** Proxies standard `errors.Is`, `errors.As`, and `errors.Join` for convenience.

### Quick Start

#### 1. Using Pre-defined Errors

The package comes with many standard HTTP errors out of the box.

```go
import "github.com/thanhbvha/go-common/xerrors"

func GetUser(id string) (*User, error) {
    if id == "" {
        return nil, xerrors.ErrBadRequest
    }
    
    user, err := db.Find(id)
    if err != nil {
        if db.IsNotFound(err) {
            // Returns a 404 NOT_FOUND error
            return nil, xerrors.ErrNotFound 
        }
        return nil, xerrors.ErrInternal
    }
    return user, nil
}
```

#### 2. Creating Custom Errors

You can define your own domain-specific errors.

```go
var ErrInsufficientFunds = xerrors.New("INSUFFICIENT_FUNDS", "You do not have enough balance", 400)
var ErrAccountLocked = xerrors.New("ACCOUNT_LOCKED", "Your account has been temporarily locked", 403)

func Withdraw(amount int) error {
    return ErrInsufficientFunds
}
```

#### 3. Wrapping Existing Errors

If a lower-level component fails (like a database or network call), wrap it to add HTTP context without losing the original stack/cause.

```go
func QueryData() error {
    err := db.Query(...)
    if err != nil {
        return xerrors.Wrap(err, "DB_QUERY_FAILED", "Failed to retrieve data from database", 500)
    }
    return nil
}
```

#### 4. Extracting Context in API Handlers (e.g. Fiber/Gin)

When an error bubbles up to your HTTP handler, you can easily extract the Status Code and String Code to format your JSON response.

```go
func ErrorMiddleware(c *fiber.Ctx, err error) error {
    statusCode := xerrors.HTTPStatusCode(err) // Defaults to 500 if not a CustomError
    stringCode := xerrors.GetCode(err)        // Defaults to "INTERNAL_ERROR"
    
    return c.Status(statusCode).JSON(fiber.Map{
        "code":    stringCode,
        "message": err.Error(),
    })
}
```

### Key Types & Functions

| Symbol | Description |
|---|---|
| `CustomError` | The underlying struct implementing the `error` interface. |
| `New(code, msg, status)` | Create a new structured error. |
| `Wrap(err, code, msg, status)` | Wrap an existing error with structured context. |
| `HTTPStatusCode(err)` | Safely extracts the `HTTPStatus` from an error (returns 500 if unknown). |
| `GetCode(err)` | Safely extracts the `Code` from an error (returns "INTERNAL_ERROR" if unknown). |
| `Is(err, target)` | Proxy for standard `errors.Is`. |
| `As(err, target)` | Proxy for standard `errors.As`. |
| `Join(errs...)` | Proxy for standard `errors.Join`. |

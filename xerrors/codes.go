package xerrors

// Standard HTTP status codes, mapped from net/http for convenience without importing it
const (
	StatusOK                  = 200
	StatusCreated             = 201
	StatusAccepted            = 202
	StatusNoContent           = 204
	StatusBadRequest          = 400
	StatusUnauthorized        = 401
	StatusForbidden           = 403
	StatusNotFound            = 404
	StatusMethodNotAllowed    = 405
	StatusNotAcceptable       = 406
	StatusRequestTimeout      = 408
	StatusConflict            = 409
	StatusPayloadTooLarge     = 413
	StatusUnsupportedMedia    = 415
	StatusUnprocessableEntity = 422
	StatusTooManyRequests     = 429
	StatusInternalServerError = 500
	StatusNotImplemented      = 501
	StatusBadGateway          = 502
	StatusServiceUnavailable  = 503
	StatusGatewayTimeout      = 504
)

// Common error variables that can be reused across services.
var (
	// 4xx Client Errors
	ErrBadRequest          = New("BAD_REQUEST", "Invalid request parameters", StatusBadRequest)
	ErrUnauthorized        = New("UNAUTHORIZED", "Authentication required", StatusUnauthorized)
	ErrInvalidToken        = New("INVALID_TOKEN", "Token is invalid or expired", StatusUnauthorized)
	ErrForbidden           = New("FORBIDDEN", "You do not have permission to access this resource", StatusForbidden)
	ErrNotFound            = New("NOT_FOUND", "Resource not found", StatusNotFound)
	ErrMethodNotAllowed    = New("METHOD_NOT_ALLOWED", "HTTP method not allowed for this resource", StatusMethodNotAllowed)
	ErrNotAcceptable       = New("NOT_ACCEPTABLE", "Requested format is not supported", StatusNotAcceptable)
	ErrRequestTimeout      = New("REQUEST_TIMEOUT", "Request took too long to process", StatusRequestTimeout)
	ErrConflict            = New("CONFLICT", "Resource already exists or conflict occurred", StatusConflict)
	ErrPayloadTooLarge     = New("PAYLOAD_TOO_LARGE", "Request payload exceeds the maximum allowed size", StatusPayloadTooLarge)
	ErrUnsupportedMedia    = New("UNSUPPORTED_MEDIA_TYPE", "Media type is not supported", StatusUnsupportedMedia)
	ErrUnprocessableEntity = New("UNPROCESSABLE_ENTITY", "Data validation failed", StatusUnprocessableEntity)
	ErrTooManyRequests     = New("TOO_MANY_REQUESTS", "Rate limit exceeded, please try again later", StatusTooManyRequests)

	// 5xx Server Errors
	ErrInternal            = New("INTERNAL_ERROR", "An unexpected internal error occurred", StatusInternalServerError)
	ErrNotImplemented      = New("NOT_IMPLEMENTED", "This feature is not yet implemented", StatusNotImplemented)
	ErrBadGateway          = New("BAD_GATEWAY", "Invalid response from upstream server", StatusBadGateway)
	ErrServiceUnavailable  = New("SERVICE_UNAVAILABLE", "Service is currently unavailable", StatusServiceUnavailable)
	ErrGatewayTimeout      = New("GATEWAY_TIMEOUT", "Upstream server timed out", StatusGatewayTimeout)
)

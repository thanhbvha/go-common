package validator

import (
	"fmt"

	"github.com/go-playground/validator/v10"
)

var validate *validator.Validate

func init() {
	validate = validator.New()
}

// ErrorResponse represents a single validation error message
type ErrorResponse struct {
	FailedField string `json:"field"`
	Tag         string `json:"tag"`
	Value       string `json:"value"`
	Message     string `json:"message"`
}

// Struct validates a struct and returns friendly error messages
func Struct(s interface{}) []*ErrorResponse {
	var errors []*ErrorResponse
	err := validate.Struct(s)
	if err != nil {
		for _, err := range err.(validator.ValidationErrors) {
			var element ErrorResponse
			element.FailedField = err.StructNamespace()
			element.Tag = err.Tag()
			element.Value = err.Param()
			element.Message = getFriendlyMessage(err)
			errors = append(errors, &element)
		}
	}
	return errors
}

func getFriendlyMessage(err validator.FieldError) string {
	switch err.Tag() {
	case "required":
		return fmt.Sprintf("Trường %s không được bỏ trống", err.Field())
	case "email":
		return "Email không đúng định dạng"
	case "min":
		return fmt.Sprintf("Trường %s phải có ít nhất %s ký tự", err.Field(), err.Param())
	case "max":
		return fmt.Sprintf("Trường %s tối đa %s ký tự", err.Field(), err.Param())
	case "len":
		return fmt.Sprintf("Trường %s phải có độ dài chính xác %s ký tự", err.Field(), err.Param())
	default:
		return fmt.Sprintf("Trường %s không hợp lệ (lỗi: %s)", err.Field(), err.Tag())
	}
}

// CustomRule allows adding a custom validation rule
func CustomRule(tag string, fn validator.Func) error {
	return validate.RegisterValidation(tag, fn)
}

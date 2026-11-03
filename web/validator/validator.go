// Package validator provides immutable, concurrency-safe request validation.
package validator

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	playground "github.com/go-playground/validator/v10"
)

// Validator owns rules registered before construction completes. Rules themselves
// must be concurrency-safe. A Validator may be shared across requests.
type Validator struct{ engine *playground.Validate }

// New builds a validator with JSON field names and optional custom rules.
// It never mutates package-global validation state.
func New(rules map[string]playground.Func) (*Validator, error) {
	v := newEngine()
	for tag, fn := range rules {
		if tag == "" || fn == nil {
			return nil, errors.New("validator: rule tag and function are required")
		}
		if err := register(v, tag, fn); err != nil {
			return nil, err
		}
	}
	return &Validator{engine: v}, nil
}

func newEngine() *playground.Validate {
	v := playground.New(playground.WithRequiredStructEnabled())
	v.RegisterTagNameFunc(func(f reflect.StructField) string {
		name := strings.SplitN(f.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return ""
		}
		if name == "" {
			return f.Name
		}
		return name
	})
	return v
}

var defaultValidator = &Validator{engine: newEngine()}

// Validate uses the immutable default rules. Use New for application rules.
func Validate(input any) ([]*ErrorResponse, error) { return defaultValidator.Validate(input) }

func register(v *playground.Validate, tag string, fn playground.Func) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("validator: invalid rule tag")
		}
	}()
	return v.RegisterValidation(tag, fn)
}

type ErrorResponse struct {
	FailedField string `json:"field"`
	Tag         string `json:"tag"`
	Value       string `json:"value"`
	Message     string `json:"message"`
}

// Validate returns field violations or an error for invalid usage/schema tags.
// Internal errors do not contain the input value.
func (v *Validator) Validate(input any) (violations []*ErrorResponse, err error) {
	if v == nil || v.engine == nil {
		return nil, errors.New("validator: instance is required")
	}
	defer func() {
		if recover() != nil {
			violations = nil
			err = errors.New("validator: invalid validation schema")
		}
	}()
	validationErr := v.engine.Struct(input)
	if validationErr == nil {
		return nil, nil
	}
	var fields playground.ValidationErrors
	if !errors.As(validationErr, &fields) {
		return nil, errors.New("validator: a non-nil struct or struct pointer is required")
	}
	for _, field := range fields {
		violations = append(violations, &ErrorResponse{FailedField: field.Field(), Tag: field.Tag(), Value: field.Param(), Message: fmt.Sprintf("Field %s failed %s validation", field.Field(), field.Tag())})
	}
	return violations, nil
}

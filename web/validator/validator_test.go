package validator

import "testing"

func TestInvalidInputsReturnError(t *testing.T) {
	v, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	var ptr *struct{ Name string }
	for _, input := range []any{nil, ptr, 42, "value"} {
		if _, err := v.Validate(input); err == nil {
			t.Fatalf("accepted invalid input %#v", input)
		}
	}
}

func TestValidationUsesJSONFields(t *testing.T) {
	input := struct {
		Email string `json:"email" validate:"required,email"`
	}{}
	v, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := v.Validate(input)
	if err != nil || len(got) != 1 || got[0].FailedField != "email" {
		t.Fatalf("validation = %+v, %v", got, err)
	}
}

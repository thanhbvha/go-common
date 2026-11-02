package str

import (
	"errors"
	"testing"
)

func TestRandom(t *testing.T) {
	value, err := Random(32)
	if err != nil {
		t.Fatalf("Random() error = %v", err)
	}
	if len(value) != 32 {
		t.Fatalf("len(Random(32)) = %d", len(value))
	}
	if _, err := Random(-1); !errors.Is(err, ErrInvalidLength) {
		t.Fatalf("Random(-1) error = %v", err)
	}
}

func TestSlugify(t *testing.T) {
	if got := Slugify("Xin chào Việt Nam"); got != "xin-chao-viet-nam" {
		t.Fatalf("Slugify() = %q", got)
	}
}

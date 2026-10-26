package registry

import (
	"errors"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestGetRecoversFactoryPanic(t *testing.T) {
	name := t.Name()
	Register(name, func() (transport.Transport, error) { panic("factory") })
	instance, err := Get(name)
	if instance != nil || !errors.Is(err, ErrFactoryPanic) {
		t.Fatalf("instance = %v, error = %v", instance, err)
	}
}

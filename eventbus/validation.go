package eventbus

import (
	"fmt"
	"strings"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func validateName(field, value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("%w: %s must not be empty", ErrInvalidName, field)
	}
	if trimmed != value {
		return fmt.Errorf("%w: %s must not have surrounding whitespace", ErrInvalidName, field)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: %s contains a control character", ErrInvalidName, field)
		}
	}
	return nil
}

func applyTopologyOptions(topology *Topology, opts []TopologyOption) error {
	for _, opt := range opts {
		if opt == nil {
			return fmt.Errorf("%w: nil topology option", ErrInvalidOption)
		}
		opt(topology)
	}
	return nil
}

func validateTopology(topology Topology, backend transport.Transport) error {
	if err := validateName("topology name", topology.JobType); err != nil {
		return err
	}
	if topology.Kind != TaskTopology && topology.Kind != EventTopology {
		return fmt.Errorf("%w: unknown topology kind %d", ErrInvalidOption, topology.Kind)
	}

	for key, value := range topology.Options {
		if strings.TrimSpace(key) == "" || key != strings.TrimSpace(key) {
			return fmt.Errorf("%w: option key %q is blank or malformed", ErrInvalidOption, key)
		}
		switch key {
		case "eventbus.concurrency", "eventbus.prefetch":
			v, ok := value.(int)
			if !ok || v <= 0 {
				return fmt.Errorf("%w: %s must be a positive int", ErrInvalidOption, key)
			}
		case "eventbus.max_attempts":
			v, ok := value.(int)
			if !ok || v <= 0 {
				return fmt.Errorf("%w: %s must be a positive int", ErrInvalidOption, key)
			}
		case "eventbus.rate_limit":
			v, ok := value.(RateLimit)
			if !ok || v.PerSecond <= 0 || v.Burst <= 0 {
				return fmt.Errorf("%w: %s requires positive rate and burst", ErrInvalidOption, key)
			}
		case "eventbus.ordering_key_concurrency":
			v, ok := value.(int)
			if !ok || v <= 0 {
				return fmt.Errorf("%w: %s must be a positive int", ErrInvalidOption, key)
			}
		case "eventbus.max_payload_bytes":
			v, ok := value.(int)
			if !ok || v <= 0 {
				return fmt.Errorf("%w: %s must be a positive int", ErrInvalidOption, key)
			}
		case transport.ResourceNameVersionOption:
			v, ok := value.(string)
			if !ok || (v != transport.ResourceNameVersionV2 && v != transport.ResourceNameVersionLegacy) {
				return fmt.Errorf("%w: %s must be %q or %q", ErrInvalidOption, key, transport.ResourceNameVersionV2, transport.ResourceNameVersionLegacy)
			}
		default:
			if strings.HasPrefix(key, "eventbus.") {
				return fmt.Errorf("%w: unknown core option %q", ErrInvalidOption, key)
			}
			prefix, _, hasPrefix := strings.Cut(key, ".")
			if hasPrefix && isBuiltInDriver(prefix) && backend != nil && prefix != backend.Name() {
				return fmt.Errorf("%w: option %q is incompatible with %s transport", ErrInvalidOption, key, backend.Name())
			}
		}
	}

	if validator, ok := backend.(transport.TopologyValidator); ok {
		if err := validator.ValidateTopology(topology); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidOption, err)
		}
	}
	return nil
}

func isBuiltInDriver(name string) bool {
	switch name {
	case "redis", "nats", "kafka", "memory":
		return true
	default:
		return false
	}
}

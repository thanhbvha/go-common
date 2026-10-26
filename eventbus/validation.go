package eventbus

import (
	"errors"
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

func applySubscriberOptions(base Topology, opts []SubscriberOption) (Topology, error) {
	topology := cloneTopology(base)
	overlay := Topology{Namespace: base.Namespace, JobType: base.JobType, Kind: base.Kind}
	if err := applyTopologyOptions(&overlay, opts); err != nil {
		return Topology{}, err
	}
	if overlay.Namespace != base.Namespace || overlay.JobType != base.JobType || overlay.Kind != base.Kind {
		return Topology{}, fmt.Errorf("%w: subscriber option changed topology identity", ErrInvalidOption)
	}
	for key, value := range overlay.Options {
		if !subscriberOptionAllowed(key) {
			return Topology{}, fmt.Errorf("%w: option %q is not valid for a subscriber", ErrInvalidOption, key)
		}
		if topology.Options == nil {
			topology.Options = make(map[string]any)
		}
		topology.Options[key] = cloneOptionValue(value)
	}
	return topology, nil
}

func subscriberOptionAllowed(key string) bool {
	switch key {
	case "eventbus.concurrency", "eventbus.prefetch", "eventbus.max_attempts", "eventbus.rate_limit", "eventbus.ordering_key_concurrency", "eventbus.concurrency_key_limit", "eventbus.max_payload_bytes",
		"redis.batch_size", "redis.block", "redis.ack_wait",
		"nats.ack_wait", "nats.batch_size", "nats.fetch_timeout",
		"kafka.fetch_max_wait", "kafka.fetch_max_bytes", "kafka.fetch_min_bytes", "kafka.fetch_max_partition_bytes", "kafka.max_buffered_records", "kafka.max_buffered_bytes":
		return true
	default:
		return false
	}
}

func cloneTopology(topology Topology) Topology {
	cloned := topology
	if topology.Options != nil {
		cloned.Options = make(map[string]any, len(topology.Options))
		for key, value := range topology.Options {
			cloned.Options[key] = cloneOptionValue(value)
		}
	}
	return cloned
}

func cloneOptionValue(value any) any {
	switch value := value.(type) {
	case map[string]string:
		cloned := make(map[string]string, len(value))
		for key, item := range value {
			cloned[key] = item
		}
		return cloned
	case []string:
		return append([]string(nil), value...)
	default:
		return value
	}
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
		case "eventbus.ordering_key_concurrency", "eventbus.concurrency_key_limit":
			v, ok := value.(int)
			if !ok || v <= 0 {
				return fmt.Errorf("%w: %s must be a positive int", ErrInvalidOption, key)
			}
		case "eventbus.max_payload_bytes":
			v, ok := value.(int)
			if !ok || v <= 0 {
				return fmt.Errorf("%w: %s must be a positive int", ErrInvalidOption, key)
			}
		case delayDisabledOption:
			v, ok := value.(bool)
			if !ok || !v {
				return fmt.Errorf("%w: %s must be true when set", ErrInvalidOption, key)
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
			if hasPrefix && isBuiltInDriver(prefix) && backend != nil {
				backendName, err := guardValue(panicScopeProvisioning, "transport name", nil, func() (string, error) { return backend.Name(), nil })
				if err != nil {
					return err
				}
				if prefix != backendName {
					return fmt.Errorf("%w: option %q is incompatible with %s transport", ErrInvalidOption, key, backendName)
				}
			}
		}
	}

	if validator, ok := backend.(transport.TopologyValidator); ok {
		if err := guardError(panicScopeProvisioning, "validate topology", nil, func() error { return validator.ValidateTopology(topology) }); err != nil {
			return errors.Join(ErrInvalidOption, err)
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

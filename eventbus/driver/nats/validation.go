package nats

import (
	"fmt"
	"strings"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func (t *natsTransport) ValidateTopology(topology transport.Topology) error {
	if err := topology.Validate(); err != nil {
		return fmt.Errorf("nats: %w", err)
	}
	if strings.ContainsAny(topology.NameWithSeparator("_"), " .*\\/>") {
		return fmt.Errorf("nats: topology name contains a reserved character")
	}
	for key, value := range topology.Options {
		switch key {
		case "nats.maxage", "nats.ack_wait", "nats.fetch_timeout", "nats.delay_lease", "nats.duplicate_window":
			v, ok := value.(time.Duration)
			if !ok || v <= 0 {
				return fmt.Errorf("nats: %s must be a positive duration", key)
			}
		case "nats.replicas":
			v, ok := value.(int)
			if !ok || v < 1 || v > 5 {
				return fmt.Errorf("nats: %s must be an int between 1 and 5", key)
			}
		case "nats.retention":
			v, ok := value.(gonats.RetentionPolicy)
			if !ok || (v != gonats.LimitsPolicy && v != gonats.InterestPolicy && v != gonats.WorkQueuePolicy) {
				return fmt.Errorf("nats: %s is invalid", key)
			}
		case "nats.batch_size", "nats.delay_batch_size":
			v, ok := value.(int)
			if !ok || v <= 0 {
				return fmt.Errorf("nats: %s must be a positive int", key)
			}
			if key == "nats.delay_batch_size" && v > 1000 {
				return fmt.Errorf("nats: %s must not exceed 1000", key)
			}
		case natsEventLayoutOption:
			if topology.Kind != transport.EventTopology || value != natsTargetedLayoutV1 {
				return fmt.Errorf("nats: %s must be %q on event topologies", key, natsTargetedLayoutV1)
			}
		default:
			if strings.HasPrefix(key, "nats.") {
				return fmt.Errorf("nats: unknown option %q", key)
			}
		}
	}
	return requireTargetedEventLayout(topology)
}

func requireTargetedEventLayout(topology transport.Topology) error {
	if topology.Kind != transport.EventTopology {
		return nil
	}
	if layout, _ := topology.Options[natsEventLayoutOption].(string); layout != natsTargetedLayoutV1 {
		return fmt.Errorf("nats: event topology requires %s=%q to exclude older broadcast-retry instances", natsEventLayoutOption, natsTargetedLayoutV1)
	}
	return nil
}

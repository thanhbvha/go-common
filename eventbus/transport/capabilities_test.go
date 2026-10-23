package transport

import (
	"testing"
	"time"
)

func TestNormalizeLegacyCapabilities(t *testing.T) {
	capabilities := NormalizeCapabilities(Capabilities{DelayPrecision: time.Second, Ordering: true})
	if !capabilities.Delay.Supported || capabilities.Delay.Precision != time.Second {
		t.Fatalf("delay = %#v", capabilities.Delay)
	}
	if !capabilities.SupportsBrokerOrdering() || capabilities.Order.Scope != OrderingPartition {
		t.Fatalf("ordering = %#v", capabilities.Order)
	}
}

package kafka

import "testing"

func TestCapabilitiesKeepLegacyMirrors(t *testing.T) {
	capabilities := New(nil, nil).Capabilities()
	if capabilities.Delay.Precision != capabilities.DelayPrecision || capabilities.Ordering != capabilities.SupportsBrokerOrdering() {
		t.Fatalf("capabilities = %#v", capabilities)
	}
}

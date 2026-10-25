package memory

import "testing"

func TestCapabilitiesKeepLegacyMirrors(t *testing.T) {
	capabilities := New().Capabilities()
	if capabilities.Delay.Precision != capabilities.DelayPrecision || capabilities.Ordering != capabilities.SupportsBrokerOrdering() {
		t.Fatalf("capabilities = %#v", capabilities)
	}
}

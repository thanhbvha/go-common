package transport

import "time"

type CapabilityScope uint8

const (
	ScopeNone CapabilityScope = iota
	ScopeProcess
	ScopeBroker
)

type DurabilityGuarantee uint8

const (
	DurabilityNone DurabilityGuarantee = iota
	DurabilityProcess
	DurabilityBroker
)

type OrderingScope uint8

const (
	OrderingNone OrderingScope = iota
	OrderingPartition
	OrderingKey
)

type SettlementGuarantee uint8

const (
	SettlementNone SettlementGuarantee = iota
	SettlementProcessLocal
	SettlementPublishBeforeSource
	SettlementAtomic
	SettlementTransactional
)

type DelayProvisioning uint8

const (
	DelayUnavailable DelayProvisioning = iota
	DelaySharedScheduler
	DelayPerTopologyScheduler
	DelayTieredPerTopology
)

type DedupeGuarantee uint8

const (
	DedupeNone DedupeGuarantee = iota
	DedupeProcessConfirmed
	DedupeBrokerConfirmed
)

type DelayCapabilities struct {
	Supported    bool
	Precision    time.Duration
	Provisioning DelayProvisioning
	Optional     bool
	Tiers        int
}

type OrderingCapabilities struct {
	Scope        OrderingScope
	Coordination CapabilityScope
}

type SettlementCapabilities struct {
	Retry SettlementGuarantee
	DLQ   SettlementGuarantee
}

type AdminCapabilities struct {
	DLQ               bool
	CursorDLQ         bool
	Stats             bool
	SubscriptionStats bool
	Pause             CapabilityScope
}

type SchedulerCapabilities struct {
	Health      bool
	WorkerScope CapabilityScope
}

type PayloadCapabilities struct {
	CoreLimitSupported bool
	BrokerMaxKnown     bool
	BrokerMaxBytes     int
}

// NormalizeCapabilities infers structured delay and ordering fields from the
// deprecated mirrors returned by older custom transports.
func NormalizeCapabilities(capabilities Capabilities) Capabilities {
	if !capabilities.Delay.Supported && capabilities.DelayPrecision > 0 {
		capabilities.Delay.Supported = true
		capabilities.Delay.Precision = capabilities.DelayPrecision
	}
	if capabilities.Delay.Precision <= 0 && capabilities.DelayPrecision > 0 {
		capabilities.Delay.Precision = capabilities.DelayPrecision
	}
	if capabilities.Order.Scope == OrderingNone && capabilities.Ordering {
		capabilities.Order.Scope = OrderingPartition
		capabilities.Order.Coordination = ScopeBroker
	}
	return capabilities
}

func (c Capabilities) SupportsBrokerOrdering() bool {
	c = NormalizeCapabilities(c)
	return c.Order.Coordination == ScopeBroker && c.Order.Scope != OrderingNone
}

func (c Capabilities) EffectiveDelayPrecision() time.Duration {
	c = NormalizeCapabilities(c)
	return c.Delay.Precision
}

package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Meter returns the global meter.
func Meter() metric.Meter {
	return otel.Meter("go-common/telemetry")
}

// MustCounter creates a new integer counter. It panics on error.
func MustCounter(name string, description string) metric.Int64Counter {
	counter, err := Meter().Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		panic(err)
	}
	return counter
}

// MustHistogram creates a new float64 histogram (e.g. for latency). It panics on error.
func MustHistogram(name string, description string) metric.Float64Histogram {
	histogram, err := Meter().Float64Histogram(name, metric.WithDescription(description))
	if err != nil {
		panic(err)
	}
	return histogram
}

// MustUpDownCounter creates a new up-down integer counter. It panics on error.
func MustUpDownCounter(name string, description string) metric.Int64UpDownCounter {
	counter, err := Meter().Int64UpDownCounter(name, metric.WithDescription(description))
	if err != nil {
		panic(err)
	}
	return counter
}

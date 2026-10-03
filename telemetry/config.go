package telemetry

import "time"

// Config holds the configuration for the Telemetry module (Tracing and Metrics).
type Config struct {
	// ServiceName is the name of the microservice (e.g., "patient-service")
	ServiceName string
	// ServiceVersion is the version of the microservice (e.g., "v1.0.0")
	ServiceVersion string
	// Environment is the environment the service is running in (e.g., "production", "staging")
	Environment string
	// Endpoint is the OTLP gRPC endpoint (e.g., "localhost:4317")
	Endpoint string
	// EnableTracing enables or disables exporting traces.
	EnableTracing bool
	// EnableMetrics enables or disables exporting metrics.
	EnableMetrics bool

	// SamplingRate controls the fraction of traces to sample (0.0 to 1.0).
	// 0 hoặc 1.0 = AlwaysSample (default, backward-compatible).
	// 0.1 = sample 10% traces — recommended for high production traffic.
	SamplingRate float64

	// Insecure disables TLS for the OTLP gRPC connection.
	// Default: true (backward-compatible). Set to false for production with TLS.
	Insecure bool

	// ExportInterval controls how often metrics are pushed to the collector.
	// Default: 15s (backward-compatible with previous hard-coded value).
	ExportInterval time.Duration
}

// DefaultConfig returns sensible defaults for local development.
func DefaultConfig(serviceName string) Config {
	return Config{
		ServiceName:    serviceName,
		ServiceVersion: "v0.0.0",
		Environment:    "development",
		Endpoint:       "localhost:4317",
		EnableTracing:  true,
		EnableMetrics:  true,
		SamplingRate:   1.0,
		Insecure:       true,
		ExportInterval: 15 * time.Second,
	}
}

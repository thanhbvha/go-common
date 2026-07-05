package telemetry

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
}

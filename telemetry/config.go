package telemetry

import (
	"crypto/tls"
	"fmt"
	"math"
	"strings"
	"time"
)

// Config holds the configuration for the Telemetry module (Tracing and Metrics).
type Config struct {
	TLSConfig *tls.Config
	Headers   map[string]string
	// EnableBaggage opts into propagation of arbitrary upstream baggage.
	EnableBaggage bool

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
	// 0 disables root sampling; 1 samples all.
	// 0.1 = sample 10% traces — recommended for high production traffic.
	SamplingRate float64

	// Insecure disables TLS for the OTLP gRPC connection.
	// Default: false. Enable explicitly for a trusted local collector.
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
		SamplingRate:   0.1,
		Insecure:       false,
		ExportInterval: 15 * time.Second,
	}
}

// Validate rejects ambiguous or unsafe exporter configuration before side effects.
func (c Config) Validate() error {
	if strings.TrimSpace(c.ServiceName) == "" {
		return fmt.Errorf("telemetry: service name required")
	}
	if math.IsNaN(c.SamplingRate) || math.IsInf(c.SamplingRate, 0) || c.SamplingRate < 0 || c.SamplingRate > 1 {
		return fmt.Errorf("telemetry: sampling rate must be between 0 and 1")
	}
	if c.Insecure && c.TLSConfig != nil {
		return fmt.Errorf("telemetry: TLSConfig conflicts with Insecure")
	}
	if (c.EnableTracing || c.EnableMetrics) && strings.TrimSpace(c.Endpoint) == "" {
		return fmt.Errorf("telemetry: exporter endpoint required")
	}
	if c.ExportInterval < 0 {
		return fmt.Errorf("telemetry: negative export interval")
	}
	return nil
}

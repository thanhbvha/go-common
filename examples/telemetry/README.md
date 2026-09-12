# Telemetry Module

## Overview
The `telemetry` module provides a unified wrapper around OpenTelemetry (`go.opentelemetry.io/otel`). It seamlessly instruments your microservice for Distributed Tracing and Metrics, sending data to any OTLP-compatible collector (e.g., Jaeger, Signoz, Grafana Tempo).

## Key Features
- **Tracing**: Track the lifecycle of a request as it moves through various components (Database, HTTP, Redis, gRPC).
- **Metrics**: Standardized Counters, Histograms, and Gauges.
- `telemetry.StartSpan(ctx, name)`: Quickly start tracing a block of code.

## 🚨 Best Practices for AI/Developers
- **Context Propagation**: The single most important rule of OpenTelemetry is Context Propagation. ALWAYS pass `context.Context` down your function calls. If you lose the context, you break the distributed trace.
- **Attributes**: Use `telemetry.SetAttributes(span, ...)` to attach high-cardinality data to spans (like `user_id` or `order_id`). This makes querying in Jaeger extremely powerful.
- **Graceful Shutdown (CRITICAL)**: Always `defer tel.Shutdown(ctx)` in your `main()` function right after initialization. OpenTelemetry batches traces asynchronously. If you exit the program without calling Shutdown, the final batch of spans will be lost in memory.

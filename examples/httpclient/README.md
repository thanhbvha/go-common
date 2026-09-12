# HTTP Client Module

## Overview
The `httpclient` module provides a highly resilient HTTP client wrapping `go-resty/resty/v2`. It is engineered for microservice communication, featuring automatic retries, exponential backoff, Circuit Breaking (via `gobreaker`), and OpenTelemetry tracing out-of-the-box.

## Key Features
- **Resilience**: `cfg.Retry` controls automatic retries for transient errors. `cfg.CircuitBreaker` prevents cascading failures when downstream services are offline.
- **Observability**: Automatically injects W3C Trace Context headers into outgoing requests.
- **Fluent API**: Retains the elegant, chainable API of Resty for setting headers, body, and query params.

## 🚨 Best Practices for AI/Developers
- **Initialization**: Create a dedicated `httpclient.Client` instance for EACH downstream service you communicate with (e.g., one for PaymentService, one for NotificationService). Do NOT share a single client globally across different domains, because Circuit Breakers are tied to the specific client instance.
- **Execution Context**: Always use `client.Execute(ctx, ...)` and pass the incoming HTTP request context (`ctx`). This is crucial for distributed tracing (OpenTelemetry) to link the outgoing request to the parent trace.

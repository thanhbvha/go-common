# Telemetry

`New` creates owned OpenTelemetry providers. It does not modify process globals.

```go
cfg := telemetry.DefaultConfig("orders") // verified TLS, 10% root sampling
cfg.Endpoint = "collector.internal:4317"
runtime, err := telemetry.New(ctx, cfg)
if err != nil { return err }
// Prefer injection for reusable components:
tracer := runtime.TracerProvider().Tracer("orders")
_ = tracer
// A process bootstrap can explicitly install the successfully built providers:
if err := runtime.InstallGlobal(); err != nil { return err }
defer runtime.Shutdown(shutdownCtx)
```

Providers and propagator are available through `TracerProvider`, `MeterProvider` and
`Propagator`. Existing span/metric helpers use global OTel providers; call `InstallGlobal`
before serving when using those helpers. Applications own the single bootstrap install;
shutdown does not restore previous process globals.

- A failed or canceled construction shuts down previously created providers in reverse
  order. Errors preserve causes with `errors.Join`.
- `SamplingRate` must be in [0,1]. Zero disables root sampling; parent sampling is honored.
- TLS is the default. `Insecure=true` is an explicit choice for a trusted local collector.
  `TLSConfig` supports private CAs/mTLS; `Headers` supports collector authentication.
- Baggage propagation is disabled unless `EnableBaggage=true`. Do not place credentials
  or user payloads in attributes or baggage.
- `Shutdown(ctx)` shares one cleanup operation and bounds each caller's wait; cleanup has
  an independent 30-second maximum. Register it as a Foundation resource before servers.
- Exporter construction may connect lazily. A successful constructor does not prove the
  collector is reachable; choose outage monitoring appropriate to the deployment.

See [reference service](../examples/production) and [production validation](../docs/PRODUCTION.md).

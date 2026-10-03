# Health Module

## Overview
The `health` module provides standard, Kubernetes-ready HTTP health check endpoints for your microservices (`/health`, `/ready`, `/live`). 

It natively supports concurrent health checks with timeouts, and provides pre-built handlers for Fiber, Gin, and Echo.

## Key Features
- **Concurrent Checks**: All registered dependency checks run simultaneously in goroutines.
- **Timeouts**: Configurable timeout per check to ensure health endpoints never hang indefinitely.
- **Adapters**: Ready-to-use handlers for `fiber` (`fiberhealth.Handler`), `gin` (`ginhealth.Handler`), and `echo` (`echohealth.Handler`) which are neatly isolated in subpackages.
- **Built-in Checkers**: Contains built-in functions for checking GORM (`GORMChecker`), Redis (`RedisChecker`), and HTTP endpoints (`HTTPChecker`).

## Covered Examples
1. `main.go`: Demonstrates how to initialize a `health.Checker`, add custom readiness and liveness checks, attach it to a Fiber application, and query the results.

## 🚨 Best Practices for AI/Developers
- **Liveness vs Readiness**: 
  - Use **Liveness** (`/live`) to check if the application process itself is running. This should be extremely lightweight (e.g. memory usage). If this fails, Kubernetes will restart the container.
  - Use **Readiness** (`/ready`) to check if the application can serve traffic (e.g. DB is up, Redis is up). If this fails, Kubernetes will remove the pod from the load balancer, but will NOT restart the container.
- **Health**: `/health` checks everything (both liveness and readiness).

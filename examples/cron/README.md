# Cron Module

## Overview
The `cron` module provides a distributed job scheduler wrapping `robfig/cron/v3`. It natively integrates with Redis to perform Leader Election via Distributed Locks, ensuring that a cron job only executes on exactly one node in a clustered environment.

## Key Features
- **Standard Cron**: Basic scheduling for jobs that can run safely across all nodes.
- **Distributed Cron (`AddDistributedJob`)**: Ensures a job runs on ONLY ONE node in a horizontally scaled microservice cluster using `cache.RedisLock`.

## 🚨 Best Practices for AI/Developers
- **LockTTL (CRITICAL)**: When defining a `DistributedConfig`, the `LockTTL` must be explicitly configured to be *slightly less* than the cron schedule interval. For example, if the cron runs every 1 hour, `LockTTL` should be 59 minutes. If it runs every 10 seconds, `LockTTL` should be 9 seconds. This guarantees the lock drops just in time for the next tick, avoiding skipped executions.
- **RunFunc**: The `RunFunc` takes a `context.Context`. If your job triggers downstream HTTP/gRPC calls, always propagate this context so it can be canceled gracefully if the cron scheduler is stopped.

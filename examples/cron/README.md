# Cron Module

## Overview
The `cron` module is a high-level wrapper around the popular `robfig/cron/v3` library. It provides second-level precision cron scheduling and adds out-of-the-box support for **Distributed Job Execution** across a cluster using Redis-backed Leader Election.

## Key Features
1. **Standard Cron**: Basic job scheduling for a single server/node (`scheduler.AddFunc`).
2. **Distributed Cron**: Ensures that a background job only runs on exactly **one node** at a time, even if you have 10 identical containers running the same application. This prevents data duplication and race conditions (`scheduler.AddDistributedJob`).

## 🚨 Best Practices for AI/Developers
- **When to use Standard vs Distributed**:
  - Use **Standard Cron** for tasks that MUST run on every node independently (e.g., clearing local memory cache, emitting instance metrics).
  - Use **Distributed Cron** for business logic tasks that interact with a central database (e.g., calculating daily reports, sending batch emails).
- **Lock TTL (CRITICAL)**: When configuring a `DistributedConfig`, the `LockTTL` **MUST** be slightly less than the cron interval (e.g., if cron runs every 10 seconds, set LockTTL to 9 seconds). If the TTL is longer than the interval, the lock will still be held when the next tick occurs, causing the job to be skipped.
- **Graceful Stop**: Always call `scheduler.Stop()` during application shutdown to ensure no new jobs are spawned while the app is closing.

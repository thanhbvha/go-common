# Queue Kafka Module

## Overview
The `queue_kafka` module provides a distributed task queue built natively on top of **Apache Kafka**. It abstracts away Kafka consumer complexities and provides a reliable job dispatching system with support for delayed jobs, automatic retries with isolated topics, and Dead Letter Queues (DLQ).

## Key Structs & Configs
- `queue_kafka.Config`: Base configuration for the queue system.
  - `TopicPrefix`: Prefix for auto-created topics (Default: `"queue"`).
  - `DefaultWorkerCount`: Number of concurrent workers for unconfigured job types.
  - `ProcessTimeout`: Maximum time allowed for a single job handler to execute.
- `queue_kafka.JobTypeOptions`: Per-job configuration defined during `RegisterJobType`.
  - `Concurrency`: Dedicated number of goroutines for this specific job type.
  - `MaxRetry`: Maximum retry attempts before routing to the DLQ.

## 🚨 Best Practices for AI/Developers
- **Task Registration (IMPORTANT)**: Always call `q.RegisterJobType(...)` and `q.RegisterHandler(...)` for all job types **before** calling `q.Start(ctx)`.
- **Auto-Provisioning**: The queue automatically calls `EnsureTopic` for all registered job types (main topic and retry topic), as well as the delayed and DLQ topics on `Start`. Ensure your Kafka user has topic creation permissions or pre-provision them.
- **Graceful Shutdown (CRITICAL)**: Call `defer q.Stop()` and block your main thread on `<-ctx.Done()`. `Stop()` signals all workers and waits up to `ShutdownTimeout` for in-flight jobs to complete gracefully.
- **Job Enqueueing**: Use `EnqueueDelayed` for jobs that should run in the future, or `EnqueueWithKey` with a partition key to ensure strict ordering of events for a specific entity (like a User ID) within a partition.

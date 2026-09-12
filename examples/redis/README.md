# Redis Module

## Overview
The `redis` module provides a thread-safe, high-performance wrapper around `go-redis/v9`. It supports Single, Sentinel, and Cluster modes natively.

## Key Structs & Configs
- `redis.Config`: Defines how to connect.
  - `Mode`: Constants like `redis.ModeSingle`, `redis.ModeSentinel`, `redis.ModeCluster`.
  - `Host`, `Port`, `Password`: For single instances.
  - `ClusterAddrs`: Array of addresses for Cluster mode.
  - `MaxConnRetries`: Ensures the application doesn't panic on transient connection issues.

## 🚨 Best Practices for AI/Developers
- **Initialization**: Use `redis.New(cfg)` and then `err := client.Connect(ctx)`. Avoid using `redis.MustConnect()` in production APIs to prevent panic on boot if Redis is temporarily unreachable.
- **Global Instance**: After a successful connection, always call `redis.SetDefault(client)`. This makes the connection pool globally accessible to caching layers and queues.
- **Graceful Shutdown (CRITICAL)**: **ALWAYS** call `defer redis.Close()` immediately after setting the default. This flushes the connections and stops background health-check goroutines safely.

# Cache & Distributed Lock Module

## Overview
The `cache` module offers a unified interface `cache.Cache` for both high-performance local memory caching (via Ristretto) and distributed caching (via Redis). Additionally, it provides a robust Distributed Lock implementation based on the Redlock algorithm.

## Key Interfaces & Structs
- `cache.Cache`: The main interface featuring `Set`, `Get`, `Delete`, and `Clear`.
- `cache.NewMemoryCache(maxCapacity)`: Initializes an in-memory cache using Ristretto.
- `cache.NewRedisCache(redisClient)`: Initializes a remote cache using go-redis.
- `cache.Lock`: Interface for distributed locks (`Acquire`, `Extend`, `Release`).

## 🚨 Best Practices for AI/Developers
- **Local vs Remote**: Use `MemoryCache` for data that changes rarely and is local to the instance (e.g., app configs). Use `RedisCache` for distributed state (e.g., user sessions).
- **Graceful Shutdown**: When using `RedisCache`, ensure the underlying Redis client is closed cleanly (`defer redisClient.Close()`).
- **Distributed Locks**: 
  - **CRITICAL**: Always ensure locks have a timeout (`TTL`) when calling `Acquire` to avoid permanent deadlocks if the worker crashes.
  - Locks are safely released using Lua scripts to guarantee atomicity. Always call `Release()` after processing.

# Rate Limit Module

## Overview
The `ratelimit` module provides a distributed, high-performance rate limiter based on the Redis Sliding Window algorithm. It prevents abuse, DDoS attacks, and ensures fair usage of APIs across a clustered environment.

## Key Features
- `ratelimit.NewRedisLimiter(redisClient, prefix)`: Initializes the core limiter engine.
- **Framework-Agnostic Middlewares**: Ready-to-use middlewares for `Fiber`, `Gin`, and `Echo` (`ratelimit.FiberMiddleware`, `ratelimit.GinMiddleware`, `ratelimit.EchoMiddleware`).
- Automatically handles Redis pipeline transactions to guarantee atomicity and speed.

## Covered Examples
This module includes four independent handler functions to demonstrate different frameworks and configurations:
1. `RunIPBasedRateLimitExample()`: Basic usage with **Fiber** where the client's IP address is used as the rate limit key.
2. `RunUserIDBasedRateLimitExample()`: A secure, production-ready example with **Fiber** that extracts a User ID from the HTTP Header to use as the rate limit key.
3. `RunGinRateLimitExample()`: Demonstrates how to use the Rate Limit middleware with the **Gin** web framework.
4. `RunEchoRateLimitExample()`: Demonstrates how to use the Rate Limit middleware with the **Echo** web framework.

## 🚨 Best Practices for AI/Developers
- **Key Generation (CRITICAL)**: By default, if you pass `nil` as the KeyGenerator to the middleware, it uses the Client's IP address. In a production environment behind load balancers, or for authenticated APIs, **always** provide a custom KeyGenerator that extracts the User ID or API Key from the context/token (as shown in `RunUserIDBasedRateLimitExample`). Relying purely on IPs can lead to NAT collisions or easy spoofing.
- **Granularity**: You can instantiate multiple limiters with different configurations (e.g., 100 req/min for general API, but 5 req/min for the `/login` endpoint to prevent brute force).

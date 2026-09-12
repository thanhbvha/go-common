# Rate Limit Module

## Overview
The `ratelimit` module provides a distributed, high-performance rate limiter based on the Redis Sliding Window algorithm. It prevents abuse, DDoS attacks, and ensures fair usage of APIs across a clustered environment.

## Key Features
- `ratelimit.NewRedisLimiter(redisClient, prefix)`: Initializes the core limiter engine.
- `ratelimit.FiberMiddleware`: A plug-and-play middleware for the Fiber web framework.
- Automatically handles Redis pipeline transactions to guarantee atomicity and speed.

## 🚨 Best Practices for AI/Developers
- **Key Generation (CRITICAL)**: By default, if you pass `nil` as the KeyGenerator to the middleware, it uses the Client's IP address. In a production environment behind load balancers, or for authenticated APIs, **always** provide a custom KeyGenerator that extracts the User ID or API Key from the context/token. Relying purely on IPs can lead to NAT collisions or easy spoofing.
- **Granularity**: You can instantiate multiple limiters with different configurations (e.g., 100 req/min for general API, but 5 req/min for the `/login` endpoint to prevent brute force).

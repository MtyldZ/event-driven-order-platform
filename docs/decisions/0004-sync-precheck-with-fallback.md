# 0004. Synchronous stock pre-check protected by a circuit breaker, with a fallback

**Status:** accepted

## Context
Without a pre-check, an order for an out-of-stock item returns `201` and is cancelled a moment later. A synchronous call to inventory-service gives an immediate `422`, but creates a runtime dependency between services.

## Decision
order-service calls `GET /stock/{sku}` before saving, with timeouts (500 ms connect, 1 s read), Resilience4j retry (3 attempts, exponential backoff) and a circuit breaker. If inventory-service can't answer, the fallback **accepts the order as `PENDING`** and the asynchronous reservation decides. A 404 (unknown SKU) is a valid answer, not a failure.

## Consequences
- Fast `422` for obviously impossible orders while inventory is healthy.
- inventory-service down does not stop order intake (verified: breaker opened after the first failing order; later orders took ~5 ms and were confirmed once inventory returned).
- The answer can be stale (it comes from the Redis cache and stock can change before reservation); the async reservation remains the source of truth. A stale-low value could reject a valid order.
- Retry only wraps an idempotent GET. The fallback sits on the outermost decorator (Retry); on the circuit breaker it would swallow failures and disable retries.

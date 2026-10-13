# 0006. No transactional outbox (yet): accept the dual-write gap on order creation

**Status:** accepted (known risk)

## Context
`POST /orders` saves the order (Postgres) and then publishes `order.created` (Kafka). The two writes are not atomic. Experiments showed: if Kafka is unavailable, `send()` can block the request thread up to 60 s and fail with HTTP 500 although the order was saved, or a crash loses the buffered event; the order then stays `PENDING` forever.

## Decision
Keep the simple "commit, then publish" for now, and make the gap detectable instead of preventing it: the stuck-orders endpoint, the timeline's `order.created` eventId, and the MCP tool `find_order_events` (which reports "never published").

## Consequences
- Simple code, no relay process.
- Orders can get stuck when Kafka is down; a human must re-publish. Clients may see a 500 for an order that exists, and a retry creates a duplicate order.
- The fix is a **transactional outbox**: write the event to an `outbox` table in the same transaction as the order, and let a relay (poller or CDC such as Debezium) publish it. It keeps at-least-once semantics, which the idempotent consumers already handle. A related improvement is an `Idempotency-Key` on `POST /orders`.

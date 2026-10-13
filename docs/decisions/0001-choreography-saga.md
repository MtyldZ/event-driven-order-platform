# 0001. Choreography-based saga for order processing

**Status:** accepted

## Context
Placing an order changes data owned by two services (orders, stock). With a database per service there is no shared transaction, and distributed transactions (2PC/XA) don't fit Kafka and couple services tightly.

## Decision
Use a saga of local transactions connected by events, **choreographed**: each service reacts to events and publishes its own; no central coordinator. A rejection is compensated by cancelling the order (`PENDING → CANCELLED`).

## Consequences
- Loose coupling: notification-service was added later without touching the other services.
- No extra component to run.
- The overall flow is not visible in one place. Mitigated with a per-order status timeline and MCP tools that explain where an order stopped.
- Nobody owns timeouts: a lost event leaves an order `PENDING` silently; detected via the stuck-orders query.
- Revisit (switch to orchestration, e.g. a state machine or a workflow engine) if the flow grows beyond a few steps, needs timeouts per step, or more compensations (e.g. payment → release stock).

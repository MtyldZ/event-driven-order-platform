# 0002. One database per service

**Status:** accepted

## Context
Services must be able to change their schema and deploy independently. Shared tables would make every schema change a cross-team coordination problem (a distributed monolith).

## Decision
Each service owns its database (`orders_db`, `inventory_db`, `notifications_db`) and its Flyway migrations. Services never read each other's tables; data is shared through events and one HTTP read endpoint. Locally all three databases run in one Postgres container.

## Consequences
- Independent schemas and deployments.
- No cross-service joins or ACID transactions; multi-service operations need a saga ([0001](0001-choreography-saga.md)) and eventual consistency.
- One container and one DB user locally is a convenience, not isolation. Production would use separate credentials (or instances) per service so the rule is enforced, not just agreed on.

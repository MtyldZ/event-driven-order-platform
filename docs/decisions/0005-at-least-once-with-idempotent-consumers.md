# 0005. At-least-once delivery with idempotent consumers

**Status:** accepted

## Context
Messages can be redelivered (consumer crash before the offset commit, rebalance) or published twice. Kafka's exactly-once (transactions) only covers Kafka-to-Kafka processing, not our Postgres writes.

## Decision
Commit offsets only after a record was processed successfully (`ack-mode=record`, Kafka auto-commit off). Every consumer stores the `eventId` it handled **in the same database transaction** as its effect and skips known ids. Listeners flush (surfacing optimistic-lock conflicts) before publishing their result. Failed records are retried with backoff and then moved to `<topic>.DLT`.

## Consequences
- Nothing is skipped; duplicates have no second effect (verified by tests and by killing a consumer mid-burst: 1 redelivery, skipped, stock exactly right).
- Every new consumer must implement the same pattern; a consumer without it would double-apply effects.
- Blocking retries hold up the affected partition (head-of-line blocking) for ~7 s per poison record; accepted to keep per-order ordering.
- DLT topics need monitoring: the error handler logs nothing on a successful DLT publish.

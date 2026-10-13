# Experiments: breaking the system on purpose

All experiments ran against the full Docker Compose stack (Kafka 4.3.1 KRaft, Postgres 18.6, Redis 8.10.2, three Spring Boot 4.1.1 services) on 2026-10-13. Numbers are what we observed, not what we expected.

Relevant settings: consumers commit offsets per record after the listener returns (`ack-mode=record`), 3 listener threads per service, 3 partitions per topic. Error handler: 3 retries with 1 s / 2 s / 4 s backoff, then `<topic>.DLT`. Producers use `acks=all` + idempotence with Kafka's default `max.block.ms=60000` and `delivery.timeout.ms=120000`.

| # | Experiment | Lost? | Duplicated? | Safeguard |
|---|---|---|---|---|
| 1 | `SIGKILL` inventory-service during 300 orders | No | 1 redelivery, no double effect | at-least-once + idempotent consumer |
| 2 | Publish the same event 5 more times | No | No effect | `processed_events` / unique `event_id` |
| 3 | Poison message | No (in DLT) | No | retries → DLT; non-retryable errors skip retries |
| 4 | Kafka down while creating orders | **Yes, 2 orders stuck** | – | none yet → transactional outbox |

---

## 1. Kill a consumer mid-message

**What we did.** Added SKU `EXP-1` with 1000 units. Sent 300 orders (1 unit each, 10 in parallel) and after 1.5 s ran `docker compose kill inventory-service` (SIGKILL: no shutdown hooks, no final offset commit; like a power cut or OOM kill). Then `docker compose start inventory-service`.

**What happened.**
- Before the kill: 4 orders reserved. The other 296 POSTs still returned `201`: the circuit breaker opened and the fallback accepted them as `PENDING`.
- Consumer group right after the kill: lag ≈ 297, and the group **still listed the 3 dead consumers**.
- inventory-service started at `11:56:00`, but partitions were only assigned at `11:56:27`.
- After restart: `Duplicate event 396ec4a2-... skipping` (1×), 296 reservations.
- Final: **300 CONFIRMED, EXP-1 = 700 (exactly 1000 − 300), 300 notifications.**

**Why.**
- One event had been processed and its DB transaction committed, but the offset commit never happened before the kill. Kafka only knows the last *committed* offset, so it redelivered that record: **at-least-once delivery** in action. The idempotency check (same transaction as the stock change) turned the redelivery into a no-op.
- The 27 s gap: a SIGKILLed consumer can't send "leave group". The broker only declares it dead after `session.timeout.ms` (45 s, kill at `11:55:42` + 45 s = `11:56:27`). Until then the group can't **rebalance** (reassign partitions), so the new consumers waited. A graceful stop (`docker compose stop`, SIGTERM) leaves the group immediately.

**Safeguard.** Commit offsets after processing (never before) + idempotent consumer. Lower `session.timeout.ms` = faster recovery from crashes but more false "dead" verdicts during GC pauses or slow processing.

## 2. Duplicates

**What we did.** Read one real `order.created` from experiment 1 and published it byte-for-byte 5 more times (same key, same `eventId`).

**What happened.** 5 log lines `Duplicate event 656cecb1-... skipping`; EXP-1 stock 700 → 700; still exactly 1 `inventory.reserved` for that order; order timeline still 2 entries.

**Why.** `processed_events.event_id` is the primary key and is checked/written in the same transaction as the stock change. Duplicates are expected in an at-least-once system (experiment 1 produced one naturally), so every consumer must tolerate them. Covered by automated tests too (`duplicateEventHasNoSecondEffect`, `duplicateInventoryResultIsIgnored`, `storesOneNotificationPerEventEvenWhenDeliveredTwice`).

## 3. Poison message: does it block the partition?

**What we did.** Published, in this order, to `order.created`:
1. key `P`: valid JSON without `payload` → `NullPointerException` in the listener (retryable)
2. key `P`: a valid order
3. keys `C1`, `C2`, `C3`: valid orders (controls)

**What happened.**

| Event | Partition | Processed after publish |
|---|---|---|
| valid, same key `P` | 2 | **8.2 s** |
| control `C2` | 2 | **8.2 s** |
| control `C1` | 0 | 0.6 s |
| control `C3` | 0 | 0.6 s |

The poison record landed in `order.created.DLT` (partition 2) with headers: `kafka_dlt-exception-fqcn = ListenerExecutionFailedException`, `kafka_dlt-exception-cause-fqcn = java.lang.NullPointerException`, `-exception-message`, full `-exception-stacktrace`, plus `-original-topic/-partition/-offset/-timestamp/-consumer-group`.
Earlier run (Phase 4): malformed JSON (`this is {not json`) reached the DLT after 0.5 s; the NPE variant after 7.6 s.

**Why.**
- Retries are **blocking**: the error handler re-seeks the partition and waits 1 + 2 + 4 s. Nothing behind the poison record in **that partition** is processed meanwhile (**head-of-line blocking**). It is per *partition*, not per key: `C2` had nothing to do with `P` but shared its partition. Partition 0 kept flowing.
- Malformed JSON is classified as non-retryable (it would fail identically every time), so it goes to the DLT immediately.
- It does not block **forever**: after the retries the record is moved to the DLT and the offset is committed. Without the DLT recoverer, Spring's default would retry 10 times and then just log and skip (data gone).

**Safeguard.** Bounded retries + DLT with full diagnostic headers. Monitor DLT topics: the error handler logs nothing on a successful DLT publish. For workloads where latency matters more than per-key ordering: non-blocking retry topics (`@RetryableTopic`).

## 4. Kafka down while creating orders (the dual-write problem)

`OrderService.create()` does two writes to two systems: `orders.save()` (Postgres, committed) and then `kafka.send()` (Kafka). There is no transaction spanning both.

**4a. Kafka down for 20 s.** `docker compose stop kafka` → POST → `201` in 70 ms, order `PENDING`, no `Published` log. `docker compose start kafka` → `Published order.created ... offset 118` → `CONFIRMED`.
*Why:* the producer still had cached metadata for `order.created`, so `send()` put the record in its **in-memory buffer** and returned. It kept retrying in the background and delivered once the broker was back (within `delivery.timeout.ms` = 120 s).

**4b. Kafka down, producer without metadata.** Same steps: the POST **hung for exactly 60 s**, then `TimeoutException: Topic order.created not present in metadata after 60000 ms` → `KafkaException: Send failed` → **HTTP 500**. But the order was **already committed**: `2ff5e3bf-...` exists as `PENDING`, with no event on Kafka.
*Why:* after the previous outage the producer had re-bootstrapped and had no metadata for the topic. Before it can pick a partition, `send()` must fetch metadata, and it blocks the *calling* thread (here: the HTTP request thread) for up to `max.block.ms`. The client sees a failure for an order that exists. If it retries, it creates a second order.

**4b′. Kafka down + order-service crash.** `stop kafka` → POST (client timeout 5 s, `send()` blocking) → `docker compose kill order-service` → start Kafka and order-service. Result: order `a85fcd70-...` in the DB as `PENDING`, **0** `order.created` records on Kafka. Nothing will ever process it.
*Why:* the event only existed in the memory of a process that died.

**4c (not run, described).** If the producer *has* metadata and Kafka stays down longer than `delivery.timeout.ms` (120 s), the buffered record expires, the future completes with a `TimeoutException`, our `whenComplete` logs `Failed to publish order.created ...`, and the order is stuck exactly like 4b′. We skipped it: same end state, 2+ minutes of waiting.

**How we noticed.** `GET /orders?status=PENDING&olderThanMinutes=0` lists both stuck orders; their timelines show a `PENDING` entry with an `order.created` eventId that never appears on the topic (the triage path from the Phase 3 checkpoint). The two orders were left in the database on purpose as real data for the MCP server (Phase 8).

**Safeguard (not implemented): transactional outbox.**
```
POST /orders ──► ONE DB transaction:  INSERT orders, order_items, order_status_history
                                      INSERT outbox (event_id, topic, key, payload)   ← the event, as a row
                 commit → 201 (the event is now as durable as the order)

relay (scheduled poller, or CDC like Debezium reading the Postgres WAL):
  SELECT * FROM outbox WHERE published_at IS NULL ORDER BY id
  → kafka.send(...).get() → UPDATE outbox SET published_at = now()
```
- Order and event are committed atomically: no stuck orders, no 500-with-a-saved-order, no events for orders that don't exist.
- The relay can crash between `send` and `UPDATE`, so it may publish twice: still **at-least-once**, which our idempotent consumers already handle (same `eventId`).
- `POST /orders` no longer touches Kafka, so a Kafka outage can't block HTTP threads.
- Costs: one more table, a relay process, a little latency (poll interval), outbox cleanup.
- Smaller, partial mitigations: lower `max.block.ms` so requests fail fast; return `202 Accepted` + order id even if the publish fails and let a repair job re-publish stuck `PENDING` orders (an outbox in disguise, with weaker guarantees).

Same problem exists in inventory-service and order-service's listeners (DB commit, then publish) but in the other direction: there the publish happens *before* the commit, so the failure mode is a duplicate result, not a lost one (see Phase 2 notes).

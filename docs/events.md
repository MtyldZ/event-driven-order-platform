# Events

All services communicate asynchronously through Kafka. This page is the contract: topics, the envelope, one real example per event, and the rules every producer and consumer follows.

## Topics

| Topic | Producer | Consumers | Meaning |
|---|---|---|---|
| `order.created` | order-service | inventory-service | A new order was accepted as `PENDING` |
| `inventory.reserved` | inventory-service | order-service | All items were reserved |
| `inventory.rejected` | inventory-service | order-service | At least one item could not be reserved; nothing was reserved |
| `order.confirmed` | order-service | notification-service | Final state: `CONFIRMED` |
| `order.cancelled` | order-service | notification-service | Final state: `CANCELLED` (compensating action) |
| `<topic>.DLT` | each consumer's error handler | humans / the MCP tool | Records that failed after retries (e.g. `order.created.DLT`) |

All topics have 3 partitions (replication factor 1: a single local broker).

## Envelope

Every event uses the same JSON envelope. Only `payload` differs per event type.

```json
{
  "eventId": "568db8ea-d781-4e80-8017-5824422ea957",
  "eventType": "order.created",
  "occurredAt": "2026-10-12T22:04:07.344029761Z",
  "orderId": "a642c9a8-8422-4ae3-b140-8f085b5cd6a5",
  "payload": { }
}
```

| Field | Purpose |
|---|---|
| `eventId` | Unique per event; **the idempotency key** consumers deduplicate on |
| `eventType` | Same as the topic name; lets one listener handle two topics (order-service on `inventory.*`) |
| `occurredAt` | When the producer created the event (UTC, ISO-8601) |
| `orderId` | The order the event belongs to; also the **Kafka record key** |
| `payload` | Event-specific data (below) |

Values are plain JSON (`JacksonJsonSerializer` with `spring.json.add.type.headers=false`): no Java class names travel with the message, so each service maps the JSON to its own classes.

## Examples (captured from the running system)

**`order.created`** (key `a642c9a8-...`)
```json
{"eventId":"568db8ea-d781-4e80-8017-5824422ea957","eventType":"order.created","occurredAt":"2026-10-12T22:04:07.344029761Z","orderId":"a642c9a8-8422-4ae3-b140-8f085b5cd6a5","payload":{"customerId":"compose-1","items":[{"sku":"SKU-1","quantity":1}]}}
```

**`inventory.reserved`**
```json
{"eventId":"6e84b3de-de7c-4eb7-9d0a-9a0e3a94bb61","eventType":"inventory.reserved","occurredAt":"2026-10-12T22:04:07.560642220Z","orderId":"a642c9a8-8422-4ae3-b140-8f085b5cd6a5","payload":{"items":[{"sku":"SKU-1","quantity":1}]}}
```

**`inventory.rejected`**
```json
{"eventId":"540785f9-e0d1-439e-b5a9-2058566a9e64","eventType":"inventory.rejected","occurredAt":"2026-10-12T19:10:35.081215Z","orderId":"bd254311-5a0e-4997-ba2a-4d443f9ac201","payload":{"reason":"Not enough stock for SKU-4 (requested 1, available 0)"}}
```

**`order.confirmed`**
```json
{"eventId":"f774d290-f9d1-4b1b-a4e4-c322df9360e9","eventType":"order.confirmed","occurredAt":"2026-10-12T22:04:07.714668845Z","orderId":"a642c9a8-8422-4ae3-b140-8f085b5cd6a5","payload":{"customerId":"compose-1","items":[{"sku":"SKU-1","quantity":1}]}}
```

**`order.cancelled`**
```json
{"eventId":"a1ca9ebd-cbc4-408a-b75d-038830dceda9","eventType":"order.cancelled","occurredAt":"2026-10-12T19:10:35.095643Z","orderId":"bd254311-5a0e-4997-ba2a-4d443f9ac201","payload":{"customerId":"cust-rj","reason":"Not enough stock for SKU-4 (requested 1, available 0)"}}
```

**`<topic>.DLT`**: the original key and value, byte for byte, on the same partition number, plus headers describing the failure:

| Header | Example |
|---|---|
| `kafka_dlt-original-topic` / `-partition` / `-offset` / `-timestamp` | `order.created`, 2, ... |
| `kafka_dlt-original-consumer-group` | `inventory-service` |
| `kafka_dlt-exception-fqcn` | `org.springframework.kafka.listener.ListenerExecutionFailedException` |
| `kafka_dlt-exception-cause-fqcn` | `java.lang.NullPointerException` |
| `kafka_dlt-exception-message`, `-exception-stacktrace` | what failed and where |

## Ordering rules

- **Key = `orderId`** for every event. Kafka sends equal keys to the same partition and keeps order within a partition, so all events of one order are seen in the order they were produced.
- There is **no ordering across orders** (different partitions are processed in parallel by 3 listener threads per service).
- Consumers process one partition sequentially; a failing record blocks the records behind it in **that partition** during its retries (1 s, 2 s, 4 s), then moves to the DLT. Measured: unrelated keys on the same partition waited 8.2 s, other partitions were unaffected ([experiments](experiments.md#3-poison-message-does-it-block-the-partition)).

## Delivery and idempotency rules

Delivery is **at-least-once**: consumers commit the offset only after the listener returned successfully (`ack-mode=record`), so a crash or rebalance can redeliver a record that was already processed. Every consumer must therefore make a duplicate harmless:

| Consumer | Deduplication | Written in the same DB transaction as |
|---|---|---|
| inventory-service (`order.created`) | `processed_events.event_id` (primary key) | the stock change |
| order-service (`inventory.*`) | `processed_events.event_id` + status guard (only `PENDING` can change) + `@Version` on orders | the status change and its history row |
| notification-service (`order.*`) | `notifications.event_id` (`UNIQUE`); the notification row is its own marker | (one row) |

Producers use `acks=all` and `enable.idempotence=true` (no duplicates from the producer's own network retries). That does **not** replace consumer idempotency: redelivery after a consumer crash, a re-published event, or the rare duplicate result described below still arrive twice.

Known gaps (documented, not fixed):
- order-service saves the order, then publishes `order.created` **outside** the database transaction. If publishing fails (Kafka down, process crash), the order stays `PENDING` with no event: the dual-write problem ([experiment 4](experiments.md#4-kafka-down-while-creating-orders-the-dual-write-problem), [ADR 0006](decisions/0006-no-transactional-outbox-yet.md)).
- Listeners publish their result before their DB transaction commits. If that commit fails, redelivery publishes the result a second time (with a new `eventId`); order-service's status guard ignores the second one.

## Evolving the contract

- Add fields; never rename or remove them (an old consumer reading a renamed field silently gets `null`).
- Consumers ignore unknown fields, so producers can add data before consumers use it.
- A breaking change needs a new event type (or topic) and a migration period where both are produced.

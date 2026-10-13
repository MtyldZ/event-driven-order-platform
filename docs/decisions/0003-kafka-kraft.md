# 0003. Apache Kafka in KRaft mode, events keyed by orderId

**Status:** accepted

## Context
The saga needs a durable log that several services read independently, keeps per-order ordering, and lets a new or restarted consumer catch up.

## Decision
Apache Kafka 4.3 (`apache/kafka` image) in **KRaft** mode: one process acting as broker and controller, no ZooKeeper (removed in Kafka 4.0). Every event uses `orderId` as the key, 3 partitions per topic, JSON values without type headers.

## Consequences
- All events of one order land in one partition and stay ordered; different orders are processed in parallel.
- Consumers keep their own offsets: a service that was down catches up (observed in experiments).
- Single broker: no replication (`replication.factor=1`). Production needs ≥ 3 brokers, `min.insync.replicas=2` and `acks=all` (already set).
- Partition count can only grow, and growing it remaps keys; chosen up front.
- Schemas are documented ([events.md](../events.md)) rather than enforced; a schema registry (Avro/Protobuf) was out of scope.

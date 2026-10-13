import { randomUUID } from 'node:crypto';
import { Admin, Consumer, ListOffsetTimestamps, MessagesStreamModes, stringDeserializers } from '@platformatic/kafka';

// Saga order: where an order's events can appear, and which group consumes each topic.
export const SAGA_TOPICS = ['order.created', 'inventory.reserved', 'inventory.rejected', 'order.confirmed', 'order.cancelled', 'order.created.DLT'];
export const SERVICE_GROUPS = ['inventory-service', 'order-service', 'notification-service'];

export interface KafkaEvent {
  topic: string;
  partition: number;
  offset: string;
  timestamp: string;
  eventType: string | null; // null when the value is not our JSON envelope (e.g. a poison message in a DLT)
  eventId: string | null;
}

export interface GroupLag {
  groupId: string;
  topic: string;
  lag: number; // summed over partitions: records on the topic the group has not committed yet
}

export interface OrderEvents {
  events: KafkaEvent[];
  lag: GroupLag[];
  complete: boolean; // false if the scan hit its time limit before reading every partition to the end
}

/** What the tool needs. Tests pass a fake object of this shape. */
export interface KafkaInspector {
  findOrderEvents(orderId: string): Promise<OrderEvents>;
}

/** An error whose message is safe to show to the model. */
export class KafkaInspectorError extends Error {}

/**
 * Read-only: a throwaway consumer group that never commits, reads from the earliest offset and is deleted afterwards.
 * ponytail: scans whole topics on every call (fine for a demo's few thousand records); with real volume,
 * search by timestamp (listOffsets with the order's createdAt) or keep an index of events per order.
 */
export class LiveKafkaInspector implements KafkaInspector {
  constructor(private readonly brokers: string[], private readonly timeoutMs = 10_000) {}

  async findOrderEvents(orderId: string): Promise<OrderEvents> {
    const admin = new Admin({ clientId: 'order-platform-mcp', bootstrapBrokers: this.brokers });
    try {
      const existing = new Set(await admin.listTopics());
      const topics = SAGA_TOPICS.filter(topic => existing.has(topic));
      const end = await this.endOffsets(admin, topics);
      const { events, complete } = await this.scan(admin, topics, end, orderId);
      return { events, lag: await this.lag(admin, end), complete };
    } catch (error) {
      if (error instanceof KafkaInspectorError) throw error;
      console.error('Kafka inspection failed:', error); // details to stderr only
      throw new KafkaInspectorError(`Kafka is not reachable at ${this.brokers.join(',')} or the inspection failed. Is the stack running?`);
    } finally {
      await admin.close().catch(() => undefined);
    }
  }

  /** "topic/partition" -> next offset that will be written (high watermark). */
  private async endOffsets(admin: Admin, topics: string[]): Promise<Map<string, bigint>> {
    const metadata = await admin.metadata({ topics });
    const listed = await admin.listOffsets({
      topics: topics.map(name => ({
        name,
        partitions: Array.from({ length: metadata.topics.get(name)?.partitionsCount ?? 0 },
          (_, partitionIndex) => ({ partitionIndex, timestamp: ListOffsetTimestamps.LATEST })),
      })),
    });
    return new Map(listed.flatMap(t => t.partitions.map(p => [`${t.name}/${p.partitionIndex}`, p.offset] as const)));
  }

  private async scan(admin: Admin, topics: string[], end: Map<string, bigint>, orderId: string) {
    const events: KafkaEvent[] = [];
    const unread = new Set([...end].filter(([, offset]) => offset > 0n).map(([key]) => key));
    if (unread.size === 0) return { events, complete: true };

    const groupId = `mcp-inspect-${randomUUID()}`;
    const consumer = new Consumer({ groupId, clientId: 'order-platform-mcp', bootstrapBrokers: this.brokers, deserializers: stringDeserializers });
    let timedOut = false;
    try {
      const stream = await consumer.consume({ topics, mode: MessagesStreamModes.EARLIEST, autocommit: false });
      const timer = setTimeout(() => { timedOut = true; void stream.close(); }, this.timeoutMs);
      try {
        for await (const message of stream) {
          if (message.key === orderId) events.push(toEvent(message));
          const key = `${message.topic}/${message.partition}`;
          if (message.offset + 1n >= (end.get(key) ?? 0n)) unread.delete(key);
          if (unread.size === 0) break;
        }
      } finally {
        clearTimeout(timer);
      }
    } finally {
      await consumer.close(true).catch(() => undefined);
      await admin.deleteGroups({ groups: [groupId] }).catch(() => undefined); // leave no trace on the broker
    }
    events.sort((a, b) => a.timestamp.localeCompare(b.timestamp)); // ISO strings sort chronologically
    return { events, complete: !timedOut };
  }

  private async lag(admin: Admin, end: Map<string, bigint>): Promise<GroupLag[]> {
    const groups = await admin.listConsumerGroupOffsets({ groups: SERVICE_GROUPS });
    const result: GroupLag[] = [];
    for (const group of groups) {
      for (const topic of group.topics) {
        let lag = 0n;
        for (const p of topic.partitions) {
          const endOffset = end.get(`${topic.name}/${p.partitionIndex}`);
          if (endOffset !== undefined && p.committedOffset >= 0n) lag += endOffset - p.committedOffset;
        }
        if (end.has(`${topic.name}/0`)) result.push({ groupId: group.groupId, topic: topic.name, lag: Number(lag) });
      }
    }
    return result;
  }
}

function toEvent(message: { topic: string; partition: number; offset: bigint; timestamp: bigint; value: string }): KafkaEvent {
  let envelope: { eventType?: string; eventId?: string } = {};
  try {
    envelope = JSON.parse(message.value);
  } catch {
    // not JSON: keep the event, just without type/id
  }
  return {
    topic: message.topic,
    partition: message.partition,
    offset: message.offset.toString(),
    timestamp: new Date(Number(message.timestamp)).toISOString(),
    eventType: envelope.eventType ?? null,
    eventId: envelope.eventId ?? null,
  };
}

/** Plain-language verdict: which saga step the order's events stop at, and why. Pure: easy to test. */
export function summarizeEvents(orderId: string, { events, lag, complete }: OrderEvents): string {
  const on = (topic: string) => events.filter(e => e.topic === topic);
  const lagOf = (groupId: string, topic: string) => lag.find(l => l.groupId === groupId && l.topic === topic)?.lag ?? 0;
  const partial = complete ? '' : ' (scan hit its time limit: result may be incomplete)';

  if (on('order.confirmed').length || on('order.cancelled').length) {
    return `Saga complete on Kafka for ${orderId}: ${on('order.confirmed').length ? 'order.confirmed' : 'order.cancelled'} was published.${partial}`;
  }
  if (on('order.created.DLT').length) {
    return `order.created for ${orderId} failed in inventory-service and was moved to order.created.DLT `
      + '(see its kafka_dlt-exception-* headers). Nothing will retry it automatically.' + partial;
  }
  if (!on('order.created').length) {
    return `No order.created event for ${orderId} exists on Kafka: it was never published. `
      + 'This is the dual-write gap (order saved in the database, event lost, e.g. Kafka was down or order-service crashed). '
      + 'Nothing will ever process this order; it needs the event re-published (a transactional outbox would prevent this).' + partial;
  }
  if (!on('inventory.reserved').length && !on('inventory.rejected').length) {
    const behind = lagOf('inventory-service', 'order.created');
    return behind > 0
      ? `order.created was published but inventory-service has not consumed it yet (lag ${behind} on order.created): is inventory-service running?${partial}`
      : `order.created was published and inventory-service is caught up, but it never published a result: check inventory-service logs for this order.${partial}`;
  }
  const behind = lagOf('order-service', 'inventory.reserved') + lagOf('order-service', 'inventory.rejected');
  return behind > 0
    ? `inventory-service answered, but order-service has not consumed the answer yet (lag ${behind}): is order-service running?${partial}`
    : `inventory-service answered and order-service is caught up, but no final event was published: check order-service logs for this order.${partial}`;
}

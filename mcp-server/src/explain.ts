import type { Timeline } from './orderApi.js';

// A healthy saga finishes in well under a second; after this long a PENDING order counts as stuck.
export const STUCK_AFTER_MINUTES = 1;

/** Plain-language summary of where an order is in the saga and what is missing. Pure: easy to test. */
export function explainTimeline(timeline: Timeline, now: Date): string {
  const created = timeline.entries.find(e => e.status === 'PENDING');
  const last = timeline.entries.at(-1);
  const ageMinutes = Math.floor((now.getTime() - Date.parse(timeline.createdAt)) / 60_000);

  if (timeline.currentStatus === 'CONFIRMED' && last) {
    return [
      `Order ${timeline.orderId} is CONFIRMED (final state).`,
      `inventory-service reserved the stock (event ${last.eventType} ${last.eventId}) at ${last.occurredAt}.`,
      'Nothing is missing; order.confirmed was published for notification-service.',
    ].join('\n');
  }

  if (timeline.currentStatus === 'CANCELLED' && last) {
    return [
      `Order ${timeline.orderId} is CANCELLED (final state).`,
      `Reason: ${last.reason ?? 'not recorded'} (event ${last.eventType} ${last.eventId}, ${last.occurredAt}).`,
      'The cancellation is the saga\'s compensating action: no stock was reserved for it.',
    ].join('\n');
  }

  if (ageMinutes < STUCK_AFTER_MINUTES) {
    return `Order ${timeline.orderId} is PENDING and was created less than a minute ago: the saga is most likely still running. Ask again in a few seconds.`;
  }

  const eventLine = created?.eventId
    ? `The order.created event that should have started the saga has eventId ${created.eventId}.`
    : 'The PENDING entry has no eventId (order created before status history existed), so the published event cannot be traced by id.';
  return [
    `Order ${timeline.orderId} is STUCK: PENDING for ${ageMinutes} minute(s) with no answer from inventory-service.`,
    'Missing step: an inventory.reserved or inventory.rejected event for this order.',
    eventLine,
    'Call find_order_events to check these on Kafka directly, or check by hand, in this order:',
    `1. Was order.created published? order-service logs "Published order.created for order ${timeline.orderId}" or "Failed to publish ...". Not on the topic = dual-write gap (order saved, event lost); nothing will retry it.`,
    '2. Is inventory-service consuming? kafka-consumer-groups --describe --group inventory-service: lag > 0 without consumers = service down.',
    `3. Did processing fail? Look for key ${timeline.orderId} in order.created.DLT.`,
    '4. Did inventory answer but order-service miss it? Check lag of group order-service on inventory.reserved / inventory.rejected.',
  ].join('\n');
}

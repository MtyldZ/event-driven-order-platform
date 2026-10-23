import {describe, expect, it} from 'vitest';
import {explainTimeline} from './explain.js';
import type {Timeline} from './orderApi.js';

const now = new Date('2026-10-13T12:00:00Z')

type TimelineEntryItem = Timeline['entries'][number]

const ORDER_ID = 'a642c9a8-8422-4ae3-b140-8f085b5cd6a5';
const CREATED: TimelineEntryItem = {
    status: 'PENDING',
    occurredAt: '2026-10-13T12:00:00Z',
    eventId: '568db8ea-d781-4e80-8017-5824422ea957',
    eventType: 'order.created',
    reason: null,
};
const CONFIRMED: TimelineEntryItem = {
    status: 'CONFIRMED',
    occurredAt: '2026-10-13T12:00:00Z',
    eventId: '568db8ea-d781-4e80-8017-5824422ea957',
    eventType: 'inventory.reserved',
    reason: null,
};
const REJECTED_WITH_NO_STOCK: TimelineEntryItem = {
    status: 'CANCELLED',
    occurredAt: '2026-10-13T12:00:00Z',
    eventId: '568db8ea-d781-4e80-8017-5824422ea957',
    eventType: 'inventory.rejected',
    reason: "No Stock",
};

const createTimeline = (entries: Timeline['entries']): Timeline => ({
    orderId: ORDER_ID,
    currentStatus: entries.at(-1)?.status || 'PENDING',
    createdAt: '2026-10-13T12:00:00Z',
    entries
})

describe('explainTimeline', () => {
    it('explains a confirmed order', () => {
        const timeline = createTimeline([CREATED, CONFIRMED]);
        expect(explainTimeline(timeline, now)).contains("is CONFIRMED (final state)")
    });

    it('explains a cancelled order with its reason', () => {
        const timeline = createTimeline([CREATED, REJECTED_WITH_NO_STOCK]);
        const response = explainTimeline(timeline, now);
        expect(response).contains("is CANCELLED (final state)");
        expect(response).contains(`Reason: ${REJECTED_WITH_NO_STOCK.reason}`);
    });

    it('a just-created order is not reported as stuck', () => {
        const timeline = createTimeline([CREATED]);
        const dateAfter10Seconds = new Date(now.getTime() + 1000 * 10);
        const response = explainTimeline(timeline, dateAfter10Seconds);
        expect(response).contains("most likely still running");
        expect(response).not.toContain("STUCK");
    });
});

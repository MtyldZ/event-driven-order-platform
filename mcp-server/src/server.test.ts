import { Client, InMemoryTransport } from '@modelcontextprotocol/client';
import { afterEach, describe, expect, it } from 'vitest';
import type { Order, OrderApi, Timeline } from './orderApi.js';
import { OrderServiceError } from './orderApi.js';
import { createServer } from './server.js';

const NOW = new Date('2026-10-13T12:30:00Z');
const STUCK_ID = '2ff5e3bf-0c2e-468f-8891-20a1582cdcfe';
const CREATED_EVENT_ID = 'baca2786-2312-4d5c-8933-fa92b63a97ae';

const stuckTimeline: Timeline = {
  orderId: STUCK_ID,
  currentStatus: 'PENDING',
  createdAt: '2026-10-13T12:00:41Z',
  entries: [{ status: 'PENDING', occurredAt: '2026-10-13T12:00:41Z', eventId: CREATED_EVENT_ID, eventType: 'order.created', reason: null }],
};

const stuckOrder: Order = { id: STUCK_ID, customerId: 'exp4b', status: 'PENDING', createdAt: '2026-10-13T12:00:41Z', items: [] };

/** A fake order-service: plain object, no mocking library needed (TypeScript only checks the shape). */
function fakeApi(overrides: Partial<OrderApi> = {}): OrderApi & { calls: string[] } {
  const calls: string[] = [];
  return {
    calls,
    getOrder: async id => { calls.push(`getOrder ${id}`); return id === STUCK_ID ? stuckOrder : null; },
    getTimeline: async id => { calls.push(`getTimeline ${id}`); return id === STUCK_ID ? stuckTimeline : null; },
    listPending: async minutes => { calls.push(`listPending ${minutes}`); return [stuckOrder]; },
    ...overrides,
  };
}

let client: Client;

/** A real MCP client talking to the real server through an in-memory pipe (no process, no stdio). */
async function connect(api: OrderApi): Promise<Client> {
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await createServer(api, () => NOW).connect(serverTransport);
  client = new Client({ name: 'test', version: '1.0.0' });
  await client.connect(clientTransport);
  return client;
}

const textOf = (result: Awaited<ReturnType<Client['callTool']>>) =>
  (result.content as { type: string; text: string }[]).map(c => c.text).join('\n');

afterEach(async () => {
  await client?.close();
});

describe('order-platform MCP server', () => {
  it('advertises exactly the three read-only tools', async () => {
    const { tools } = await (await connect(fakeApi())).listTools();

    expect(tools.map(t => t.name).sort()).toEqual(['explain_order_timeline', 'get_order_status', 'list_stuck_orders']);
    expect(tools.every(t => t.annotations?.readOnlyHint === true)).toBe(true);
  });

  it('rejects an orderId that is not a UUID before calling order-service', async () => {
    const api = fakeApi();
    const result = await (await connect(api)).callTool({ name: 'get_order_status', arguments: { orderId: '../../actuator' } });

    expect(result.isError).toBe(true);
    expect(textOf(result)).toContain('Input validation error');
    expect(api.calls).toEqual([]); // the boundary held: nothing reached order-service
  });

  it('rejects a negative olderThanMinutes', async () => {
    const api = fakeApi();
    const result = await (await connect(api)).callTool({ name: 'list_stuck_orders', arguments: { olderThanMinutes: -1 } });

    expect(result.isError).toBe(true);
    expect(api.calls).toEqual([]);
  });

  it('reports an unknown order as a readable tool error', async () => {
    const result = await (await connect(fakeApi())).callTool({
      name: 'get_order_status', arguments: { orderId: '00000000-0000-4000-8000-000000000000' },
    });

    expect(result.isError).toBe(true);
    expect(textOf(result)).toBe('No order with id 00000000-0000-4000-8000-000000000000.');
  });

  it('explains a stuck order: what is missing and the event id to trace', async () => {
    const result = await (await connect(fakeApi())).callTool({ name: 'explain_order_timeline', arguments: { orderId: STUCK_ID } });

    expect(result.isError).toBeFalsy();
    const text = textOf(result);
    expect(text).toContain('is STUCK: PENDING for 29 minute(s)');
    expect(text).toContain('Missing step: an inventory.reserved or inventory.rejected event');
    expect(text).toContain(CREATED_EVENT_ID);
  });

  it('uses 5 minutes when olderThanMinutes is omitted', async () => {
    const api = fakeApi();
    const result = await (await connect(api)).callTool({ name: 'list_stuck_orders', arguments: {} });

    expect(api.calls).toEqual(['listPending 5']);
    expect(textOf(result)).toContain(STUCK_ID);
  });

  it('never leaks stack traces when order-service is down or something unexpected breaks', async () => {
    const down = await (await connect(fakeApi({
      getOrder: async () => { throw new OrderServiceError('order-service is not reachable at http://localhost:8081.'); },
    }))).callTool({ name: 'get_order_status', arguments: { orderId: STUCK_ID } });
    await client.close();
    const bug = await (await connect(fakeApi({
      getTimeline: async () => { throw new TypeError("Cannot read properties of undefined (reading 'entries')"); },
    }))).callTool({ name: 'explain_order_timeline', arguments: { orderId: STUCK_ID } });

    expect(down.isError).toBe(true);
    expect(textOf(down)).toBe('order-service is not reachable at http://localhost:8081.');
    expect(bug.isError).toBe(true);
    expect(textOf(bug)).toBe('Internal error in the MCP server. Details are in its stderr log.');
    expect(textOf(bug)).not.toMatch(/at .*\.(ts|js):\d+|TypeError/);
  });
});

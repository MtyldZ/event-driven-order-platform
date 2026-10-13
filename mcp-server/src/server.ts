import { McpServer } from '@modelcontextprotocol/server';
import * as z from 'zod/v4';
import { explainTimeline } from './explain.js';
import { type KafkaInspector, KafkaInspectorError, summarizeEvents } from './kafkaInspector.js';
import { type OrderApi, OrderServiceError } from './orderApi.js';

// The schemas are the security boundary: the SDK rejects bad arguments before any handler runs,
// so nothing the model invents (paths, huge numbers, SQL) ever reaches order-service.
const orderId = z.uuid().describe('Order id (UUID), e.g. from POST /orders or list_stuck_orders');

// All tools only read. The hint lets clients skip confirmation prompts and tells the model they're safe to call.
const readOnly = { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false };

type ToolResult = { content: { type: 'text'; text: string }[]; isError?: boolean };

const text = (value: string): ToolResult => ({ content: [{ type: 'text', text: value }] });
const failure = (value: string): ToolResult => ({ content: [{ type: 'text', text: value }], isError: true });

/** Turns any thrown error into a tool error the model can read, without stack traces or internals. */
async function safely(action: () => Promise<ToolResult>): Promise<ToolResult> {
  try {
    return await action();
  } catch (error) {
    if (error instanceof OrderServiceError || error instanceof KafkaInspectorError) return failure(error.message);
    console.error('Unexpected tool error:', error);
    return failure('Internal error in the MCP server. Details are in its stderr log.');
  }
}

export function createServer(api: OrderApi, kafka: KafkaInspector, now: () => Date = () => new Date()): McpServer {
  const server = new McpServer({ name: 'order-platform', version: '0.1.0' });

  server.registerTool(
    'get_order_status',
    {
      description: 'Get the current status (PENDING, CONFIRMED, CANCELLED), customer, items and creation time of one order.',
      inputSchema: z.object({ orderId }),
      annotations: readOnly,
    },
    ({ orderId }) => safely(async () => {
      const order = await api.getOrder(orderId);
      return order ? text(JSON.stringify(order, null, 2)) : failure(`No order with id ${orderId}.`);
    }),
  );

  server.registerTool(
    'list_stuck_orders',
    {
      description: 'List orders that are still PENDING and were created more than `olderThanMinutes` ago. '
        + 'A healthy order leaves PENDING within a second, so these are candidates for a broken saga.',
      inputSchema: z.object({
        olderThanMinutes: z.number().int().min(0).max(10_080).default(5)
          .describe('Minimum age in minutes (0 = all PENDING orders, max 10080 = one week)'),
      }),
      annotations: readOnly,
    },
    ({ olderThanMinutes }) => safely(async () => {
      const orders = await api.listPending(olderThanMinutes);
      if (orders.length === 0) return text(`No PENDING orders older than ${olderThanMinutes} minute(s).`);
      const lines = orders.map(o => `- ${o.id}  customer=${o.customerId}  createdAt=${o.createdAt}`);
      return text(`${orders.length} PENDING order(s) older than ${olderThanMinutes} minute(s):\n${lines.join('\n')}`);
    }),
  );

  server.registerTool(
    'explain_order_timeline',
    {
      description: 'Explain where an order is in the saga (order.created -> inventory -> confirmed/cancelled), '
        + 'what is missing if it is stuck, and how to investigate. Returns a summary plus the raw status history.',
      inputSchema: z.object({ orderId }),
      annotations: readOnly,
    },
    ({ orderId }) => safely(async () => {
      const timeline = await api.getTimeline(orderId);
      if (!timeline) return failure(`No order with id ${orderId}.`);
      return text(`${explainTimeline(timeline, now())}\n\nStatus history:\n${JSON.stringify(timeline.entries, null, 2)}`);
    }),
  );

  server.registerTool(
    'find_order_events',
    {
      description: 'Search Kafka for every event of one order (order.created, inventory.*, order.confirmed/cancelled, '
        + 'order.created.DLT) and the consumer-group lag of each service, then say at which saga step the events stop and why. '
        + 'Use it to check on Kafka what explain_order_timeline can only suspect. Read-only; takes a few seconds.',
      inputSchema: z.object({ orderId }),
      annotations: readOnly,
    },
    ({ orderId }) => safely(async () => {
      // Kafka alone can't tell "never published" from "never existed": ask order-service first.
      if (!(await api.getOrder(orderId))) return failure(`No order with id ${orderId}.`);
      const result = await kafka.findOrderEvents(orderId);
      return text(`${summarizeEvents(orderId, result)}\n\nEvents (by key = orderId):\n${JSON.stringify(result.events, null, 2)}`
        + `\n\nConsumer lag:\n${result.lag.map(l => `- ${l.groupId} on ${l.topic}: ${l.lag}`).join('\n')}`);
    }),
  );

  return server;
}

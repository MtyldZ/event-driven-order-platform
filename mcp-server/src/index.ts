import { serveStdio } from '@modelcontextprotocol/server/stdio';
import { HttpOrderApi } from './orderApi.js';
import { createServer } from './server.js';

const baseUrl = process.env.ORDER_SERVICE_URL ?? 'http://localhost:8081';

// stdout is the JSON-RPC channel: any console.log would corrupt it. Everything human goes to stderr.
void serveStdio(() => createServer(new HttpOrderApi(baseUrl)));
console.error(`order-platform MCP server on stdio, order-service at ${baseUrl}`);

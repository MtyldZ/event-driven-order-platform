import { serveStdio } from '@modelcontextprotocol/server/stdio';
import { LiveKafkaInspector } from './kafkaInspector.js';
import { HttpOrderApi } from './orderApi.js';
import { createServer } from './server.js';

const baseUrl = process.env.ORDER_SERVICE_URL ?? 'http://localhost:8081';
const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

// stdout is the JSON-RPC channel: any console.log would corrupt it. Everything human goes to stderr.
void serveStdio(() => createServer(new HttpOrderApi(baseUrl), new LiveKafkaInspector(brokers)));
console.error(`order-platform MCP server on stdio, order-service at ${baseUrl}, Kafka at ${brokers.join(',')}`);

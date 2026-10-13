// Read-only client for order-service's REST API. The only system the MCP server talks to.

export type OrderStatus = 'PENDING' | 'CONFIRMED' | 'CANCELLED';

export interface Order {
  id: string;
  customerId: string;
  status: OrderStatus;
  createdAt: string;
  items: { sku: string; quantity: number }[];
}

export interface TimelineEntry {
  status: OrderStatus;
  occurredAt: string;
  eventId: string | null;
  eventType: string | null;
  reason: string | null;
}

export interface Timeline {
  orderId: string;
  currentStatus: OrderStatus;
  createdAt: string;
  entries: TimelineEntry[];
}

/** What the tools need. Tests pass a fake object of this shape instead of the HTTP client. */
export interface OrderApi {
  getOrder(id: string): Promise<Order | null>;
  getTimeline(id: string): Promise<Timeline | null>;
  listPending(olderThanMinutes: number): Promise<Order[]>;
}

/** An error whose message is safe to show to the model (no stack trace, no internals). */
export class OrderServiceError extends Error {}

export class HttpOrderApi implements OrderApi {
  constructor(private readonly baseUrl: string, private readonly timeoutMs = 5000) {}

  getOrder(id: string) {
    return this.get<Order>(`/orders/${encodeURIComponent(id)}`);
  }

  getTimeline(id: string) {
    return this.get<Timeline>(`/orders/${encodeURIComponent(id)}/timeline`);
  }

  async listPending(olderThanMinutes: number) {
    return (await this.get<Order[]>(`/orders?status=PENDING&olderThanMinutes=${olderThanMinutes}`)) ?? [];
  }

  /** null for 404; OrderServiceError for anything else that is not a 2xx. */
  private async get<T>(path: string): Promise<T | null> {
    let response: Response;
    try {
      response = await fetch(this.baseUrl + path, { signal: AbortSignal.timeout(this.timeoutMs) });
    } catch (error) {
      console.error(`GET ${path} failed:`, error); // details go to stderr (host logs), never to the model
      throw new OrderServiceError(`order-service is not reachable at ${this.baseUrl}. Is the stack running (docker compose ps)?`);
    }
    if (response.status === 404) return null;
    if (!response.ok) {
      console.error(`GET ${path} -> HTTP ${response.status}: ${await response.text()}`);
      throw new OrderServiceError(`order-service answered HTTP ${response.status} for ${path}.`);
    }
    return (await response.json()) as T;
  }
}

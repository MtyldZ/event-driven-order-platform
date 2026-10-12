package com.example.platform.inventory;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Event contracts as seen by inventory-service (same JSON envelope as every service, see docs/events.md). */
final class Events {

    static final String ORDER_CREATED = "order.created";
    static final String INVENTORY_RESERVED = "inventory.reserved";
    static final String INVENTORY_REJECTED = "inventory.rejected";

    private Events() {
    }

    record EventEnvelope<T>(UUID eventId, String eventType, Instant occurredAt, UUID orderId, T payload) {

        static <T> EventEnvelope<T> of(String eventType, UUID orderId, T payload) {
            return new EventEnvelope<>(UUID.randomUUID(), eventType, Instant.now(), orderId, payload);
        }
    }

    record Item(String sku, int quantity) {
    }

    record OrderCreated(String customerId, List<Item> items) {
    }

    record InventoryReserved(List<Item> items) {
    }

    record InventoryRejected(String reason) {
    }
}

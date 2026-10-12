package com.example.platform.order;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record TimelineResponse(UUID orderId, OrderStatus currentStatus, Instant createdAt, List<Entry> entries) {

    public record Entry(OrderStatus status, Instant occurredAt, UUID eventId, String eventType, String reason) {
    }

    static TimelineResponse from(Order order) {
        List<Entry> entries = order.getHistory().stream()
                .map(h -> new Entry(h.getStatus(), h.getOccurredAt(), h.getEventId(), h.getEventType(), h.getReason()))
                .toList();
        return new TimelineResponse(order.getId(), order.getStatus(), order.getCreatedAt(), entries);
    }
}

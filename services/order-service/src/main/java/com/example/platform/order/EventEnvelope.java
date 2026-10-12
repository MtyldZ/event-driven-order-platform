package com.example.platform.order;

import java.time.Instant;
import java.util.UUID;

/** Shared JSON shape of every event on every topic (see docs/events.md). */
public record EventEnvelope<T>(UUID eventId, String eventType, Instant occurredAt, UUID orderId, T payload) {

    public static <T> EventEnvelope<T> of(String eventType, UUID orderId, T payload) {
        return new EventEnvelope<>(UUID.randomUUID(), eventType, Instant.now(), orderId, payload);
    }
}

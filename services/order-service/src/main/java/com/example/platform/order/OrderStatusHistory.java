package com.example.platform.order;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "order_status_history")
public class OrderStatusHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "order_id")
    private Order order;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    private UUID eventId;

    private String eventType;

    private String reason;

    @Column(nullable = false)
    private Instant occurredAt;

    protected OrderStatusHistory() {
        // for JPA
    }

    OrderStatusHistory(Order order, OrderStatus status, UUID eventId, String eventType, String reason) {
        this.order = order;
        this.status = status;
        this.eventId = eventId;
        this.eventType = eventType;
        this.reason = reason;
        this.occurredAt = Instant.now();
    }

    public OrderStatus getStatus() { return status; }
    public UUID getEventId() { return eventId; }
    public String getEventType() { return eventType; }
    public String getReason() { return reason; }
    public Instant getOccurredAt() { return occurredAt; }
}

package com.example.platform.notification;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notifications")
public class Notification {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private UUID eventId;

    @Column(nullable = false)
    private UUID orderId;

    @Column(nullable = false)
    private String customerId;

    @Column(nullable = false)
    private String type;

    @Column(nullable = false)
    private String message;

    @Column(nullable = false)
    private Instant createdAt;

    protected Notification() {
        // for JPA
    }

    public Notification(UUID eventId, UUID orderId, String customerId, String type, String message) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.type = type;
        this.message = message;
        this.createdAt = Instant.now();
    }
}

package com.example.platform.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/** "Sends" a notification for every final order event. Here: a log line + a stored row. */
@Component
class OrderOutcomeListener {

    record EventEnvelope(UUID eventId, String eventType, Instant occurredAt, UUID orderId, Payload payload) {
    }

    /** order.confirmed carries items, order.cancelled carries a reason; we only need these two fields. */
    record Payload(String customerId, String reason) {
    }

    private static final Logger log = LoggerFactory.getLogger(OrderOutcomeListener.class);

    private final NotificationRepository notifications;

    OrderOutcomeListener(NotificationRepository notifications) {
        this.notifications = notifications;
    }

    @KafkaListener(topics = {"order.confirmed", "order.cancelled"})
    @Transactional
    public void onOrderOutcome(EventEnvelope event) {
        if (notifications.existsByEventId(event.eventId())) {
            log.info("Duplicate event {} for order {}, skipping", event.eventId(), event.orderId());
            return;
        }
        String reason = event.payload().reason();
        String message = "order.confirmed".equals(event.eventType())
                ? "Your order " + event.orderId() + " is confirmed."
                : "Your order " + event.orderId() + " was cancelled" + (reason != null ? ": " + reason : ".");

        // ponytail: "sending" = log line. A real email/SMS call would go here, ideally after commit.
        notifications.save(new Notification(event.eventId(), event.orderId(), event.payload().customerId(), event.eventType(), message));
        log.info("Notify {}: {}", event.payload().customerId(), message);
    }
}

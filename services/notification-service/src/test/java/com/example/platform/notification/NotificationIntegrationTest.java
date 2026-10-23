package com.example.platform.notification;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class NotificationIntegrationTest {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired JdbcTemplate jdbc;

    @Test
    void storesOneNotificationPerEventEvenWhenDeliveredTwice() {
        UUID orderId = UUID.randomUUID();
        String event = """
                {"eventId":"%s","eventType":"order.cancelled","occurredAt":"2026-01-01T12:00:00Z","orderId":"%s",
                 "payload":{"customerId":"cust-test","reason":"Not enough stock for SKU-4"}}
                """.formatted(UUID.randomUUID(), orderId);

        kafka.send("order.cancelled", orderId.toString(), event);
        kafka.send("order.cancelled", orderId.toString(), event);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(countMessagesFor(orderId)).isEqualTo(1));
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(countMessagesFor(orderId)).isEqualTo(1));
        assertThat(jdbc.queryForObject("SELECT message FROM notifications WHERE order_id = ?", String.class, orderId))
                .isEqualTo("Your order " + orderId + " was cancelled: Not enough stock for SKU-4");
    }

    @Test
    void storesOneConfirmedNotification() {
        UUID orderId = UUID.randomUUID();
        String event = """
                {"eventId":"%s","eventType":"order.confirmed","occurredAt":"2026-01-01T12:00:00Z","orderId":"%s",
                 "payload":{"customerId":"cust-test","items": [{"sku":"SKU-1","quantity":1}]}}
                """.formatted(UUID.randomUUID(), orderId);

        kafka.send("order.confirmed", orderId.toString(), event);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(countMessagesFor(orderId)).isEqualTo(1));
        assertThat(jdbc.queryForObject("SELECT message FROM notifications WHERE order_id = ?", String.class, orderId))
                .isEqualTo("Your order " + orderId + " is confirmed.");
    }

    private int countMessagesFor(UUID orderId) {
        return jdbc.queryForObject("SELECT count(*) FROM notifications WHERE order_id = ?", Integer.class, orderId);
    }
}

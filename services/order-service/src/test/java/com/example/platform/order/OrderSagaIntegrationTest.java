package com.example.platform.order;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * order-service against real Kafka + Postgres. The test plays inventory-service's part
 * by publishing inventory.reserved / inventory.rejected itself.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OrderSagaIntegrationTest {

    // No inventory-service here. A Mockito mock returns Optional.empty() by default = "inventory could not answer".
    @MockitoBean InventoryClient inventoryClient;

    @Autowired OrderService orderService;
    @Autowired KafkaTemplate<String, Object> kafka;
    @Autowired KafkaContainer kafkaContainer;

    KafkaConsumer<String, String> events;
    final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    @BeforeEach
    void setUp() {
        events = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new StringDeserializer());
        events.subscribe(List.of(OrderService.ORDER_CREATED, InventoryResultListener.ORDER_CONFIRMED, InventoryResultListener.ORDER_CANCELLED));
    }

    @AfterEach
    void tearDown() {
        events.close();
    }

    @Test
    void happyPathConfirmsTheOrder() {
        OrderResponse order = createOrder();

        // 1. order.created is published, and its eventId is the one recorded in the PENDING history entry.
        UUID createdEventId = orderService.timeline(order.id()).entries().getFirst().eventId();
        await().atMost(Duration.ofSeconds(15)).pollInSameThread().untilAsserted(() ->
                assertThat(eventsFor(order, OrderService.ORDER_CREATED)).singleElement()
                        .satisfies(r -> assertThat(r.value()).contains(createdEventId.toString())));

        // 2. inventory answers "reserved".
        publish(InventoryResultListener.INVENTORY_RESERVED, order, Map.of());

        // 3. order becomes CONFIRMED and order.confirmed is published.
        await().atMost(Duration.ofSeconds(15)).pollInSameThread().untilAsserted(() -> {
            assertThat(orderService.get(order.id()).status()).isEqualTo(OrderStatus.CONFIRMED);
            assertThat(eventsFor(order, InventoryResultListener.ORDER_CONFIRMED)).hasSize(1);
        });
    }

    @Test
    void rejectionCancelsTheOrderWithTheReason() {
        OrderResponse order = createOrder();

        publish(InventoryResultListener.INVENTORY_REJECTED, order, Map.of("reason", "Not enough stock for SKU-1"));

        await().atMost(Duration.ofSeconds(15)).pollInSameThread().untilAsserted(() -> {
            TimelineResponse timeline = orderService.timeline(order.id());
            assertThat(timeline.currentStatus()).isEqualTo(OrderStatus.CANCELLED);
            assertThat(timeline.entries().getLast().reason()).isEqualTo("Not enough stock for SKU-1");
            assertThat(eventsFor(order, InventoryResultListener.ORDER_CANCELLED)).hasSize(1);
        });
    }

    @Test
    void duplicateInventoryResultIsIgnored() {
        OrderResponse order = createOrder();
        var reserved = EventEnvelope.of(InventoryResultListener.INVENTORY_RESERVED, order.id(), Map.of());

        kafka.send(reserved.eventType(), order.id().toString(), reserved);
        kafka.send(reserved.eventType(), order.id().toString(), reserved); // same eventId

        await().atMost(Duration.ofSeconds(15)).pollInSameThread().untilAsserted(() ->
                assertThat(eventsFor(order, InventoryResultListener.ORDER_CONFIRMED)).hasSize(1));
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).pollInSameThread().untilAsserted(() -> {
            assertThat(orderService.timeline(order.id()).entries()).hasSize(2); // PENDING, CONFIRMED
            assertThat(eventsFor(order, InventoryResultListener.ORDER_CONFIRMED)).hasSize(1);
        });
    }

    private OrderResponse createOrder() {
        return orderService.create(new CreateOrderRequest("cust-test", List.of(new CreateOrderRequest.Item("SKU-1", 1))));
    }

    private void publish(String eventType, OrderResponse order, Map<String, Object> payload) {
        kafka.send(eventType, order.id().toString(), EventEnvelope.of(eventType, order.id(), payload));
    }

    private List<ConsumerRecord<String, String>> eventsFor(OrderResponse order, String topic) {
        events.poll(Duration.ofMillis(200)).forEach(received::add);
        return received.stream()
                .filter(r -> r.topic().equals(topic) && r.key().equals(order.id().toString()))
                .toList();
    }
}

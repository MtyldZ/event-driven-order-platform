package com.example.platform.inventory;

import com.example.platform.inventory.Events.EventEnvelope;
import com.example.platform.inventory.Events.Item;
import com.example.platform.inventory.Events.OrderCreated;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class ReservationIntegrationTest {

    @Autowired KafkaTemplate<String, Object> kafka;
    @Autowired KafkaContainer kafkaContainer;
    @Autowired JdbcTemplate jdbc;
    @Autowired StockRepository stocks;
    @Autowired PlatformTransactionManager transactionManager;

    // Every test gets its own SKU, so tests never see each other's stock changes.
    String sku;
    KafkaConsumer<String, String> results;
    final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    @BeforeEach
    void setUp() {
        sku = "TEST-" + UUID.randomUUID();
        jdbc.update("INSERT INTO stock (sku, available) VALUES (?, 5)", sku);

        // A separate consumer (own group) that reads what inventory-service publishes.
        results = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new StringDeserializer());
        results.subscribe(List.of(Events.INVENTORY_RESERVED, Events.INVENTORY_REJECTED));
    }

    @AfterEach
    void tearDown() {
        results.close();
    }

    @Test
    void reservesStockAndPublishesInventoryReserved() {
        var event = orderCreated(2);

        kafka.send(Events.ORDER_CREATED, event.orderId().toString(), event);

        // Async: poll until the condition holds (or fail after 15s). No Thread.sleep guessing.
        await().atMost(Duration.ofSeconds(15)).pollInSameThread().untilAsserted(() -> {
            assertThat(available()).isEqualTo(3);
            assertThat(resultsFor(event)).singleElement().satisfies(r -> assertThat(r.topic()).isEqualTo(Events.INVENTORY_RESERVED));
        });
    }

    @Test
    void rejectsWhenNotEnoughStockAndChangesNothing() {
        var event = orderCreated(6);

        kafka.send(Events.ORDER_CREATED, event.orderId().toString(), event);

        await().atMost(Duration.ofSeconds(15)).pollInSameThread().untilAsserted(() ->
                assertThat(resultsFor(event)).singleElement().satisfies(r -> {
                    assertThat(r.topic()).isEqualTo(Events.INVENTORY_REJECTED);
                    assertThat(r.value()).contains("Not enough stock for " + sku + " (requested 6, available 5)");
                }));
        assertThat(available()).isEqualTo(5);
    }

    @Test
    void duplicateEventHasNoSecondEffect() {
        var event = orderCreated(2);

        kafka.send(Events.ORDER_CREATED, event.orderId().toString(), event);
        kafka.send(Events.ORDER_CREATED, event.orderId().toString(), event); // exact same eventId

        await().atMost(Duration.ofSeconds(15)).pollInSameThread().untilAsserted(() -> assertThat(resultsFor(event)).hasSize(1));
        // Proving that something does NOT happen needs a time window: it must stay true for 3 seconds.
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).pollInSameThread().untilAsserted(() -> {
            assertThat(available()).isEqualTo(3);
            assertThat(resultsFor(event)).hasSize(1);
        });
    }

    @Test
    void staleUpdateOfTheSameStockRowFailsWithOptimisticLock() {
        var outer = new TransactionTemplate(transactionManager);
        var inner = new TransactionTemplate(transactionManager);
        inner.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        // Deterministic version of "two orders race for the same stock", in one thread:
        assertThatThrownBy(() -> outer.executeWithoutResult(tx -> {
            Stock mine = stocks.findById(sku).orElseThrow();                                         // A reads version 0
            inner.executeWithoutResult(other -> stocks.findById(sku).orElseThrow().reserve(1)); // B commits version 1
            mine.reserve(1);                                                                         // A writes based on version 0
        })).isInstanceOf(ObjectOptimisticLockingFailureException.class);                             // ... at commit: 0 rows matched

        assertThat(available()).isEqualTo(4); // only B's reservation happened
    }

    private EventEnvelope<OrderCreated> orderCreated(int quantity) {
        return EventEnvelope.of(Events.ORDER_CREATED, UUID.randomUUID(), new OrderCreated("cust-test", List.of(new Item(sku, quantity))));
    }

    private int available() {
        return jdbc.queryForObject("SELECT available FROM stock WHERE sku = ?", Integer.class, sku);
    }

    private List<ConsumerRecord<String, String>> resultsFor(EventEnvelope<?> event) {
        results.poll(Duration.ofMillis(200)).forEach(received::add);
        return received.stream().filter(r -> r.key().equals(event.orderId().toString())).toList();
    }
}

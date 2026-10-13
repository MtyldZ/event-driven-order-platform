package com.example.platform.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class OrderService {

    static final String ORDER_CREATED = "order.created";

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orders;
    private final KafkaTemplate<String, Object> kafka;
    private final InventoryClient inventory;

    OrderService(OrderRepository orders, KafkaTemplate<String, Object> kafka, InventoryClient inventory) {
        this.orders = orders;
        this.kafka = kafka;
        this.inventory = inventory;
    }

    // Deliberately NOT @Transactional: save() runs in its own transaction and commits when it returns,
    // so we only publish order.created for an order that really exists in the database.
    // ponytail: if Kafka is down after the commit, the order stays PENDING with no event (dual-write problem,
    // explored in Phase 6; transactional outbox is the fix).
    public OrderResponse create(CreateOrderRequest request) {
        rejectIfObviouslyOutOfStock(request);

        // The event id is chosen up front so the PENDING history entry can point at the event that announces it.
        UUID eventId = UUID.randomUUID();
        Order order = new Order(request.customerId(), eventId);
        request.items().forEach(item -> order.addItem(item.sku(), item.quantity()));
        orders.save(order);

        var event = new EventEnvelope<>(eventId, ORDER_CREATED, order.getCreatedAt(), order.getId(), OrderPayload.from(order));
        // Key = orderId: every event of this order goes to the same partition, so they stay in order.
        kafka.send(ORDER_CREATED, order.getId().toString(), event).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish {} for order {}", ORDER_CREATED, order.getId(), ex);
            } else {
                log.info("Published {} for order {} -> partition {} offset {}", ORDER_CREATED, order.getId(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
        return OrderResponse.from(order);
    }

    // Fail fast with 422 when inventory already says "not enough". This is only a pre-check:
    // stock can change before the async reservation, which stays the real decision.
    private void rejectIfObviouslyOutOfStock(CreateOrderRequest request) {
        Map<String, Integer> requested = request.items().stream()
                .collect(Collectors.toMap(CreateOrderRequest.Item::sku, CreateOrderRequest.Item::quantity, Integer::sum, TreeMap::new));
        requested.forEach((sku, quantity) -> inventory.availableStock(sku).ifPresent(available -> {
            if (available < quantity) {
                throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_CONTENT,
                        "Not enough stock for %s (requested %d, available %d)".formatted(sku, quantity, available));
            }
        }));
    }

    @Transactional(readOnly = true)
    public OrderResponse get(UUID id) {
        return OrderResponse.from(find(id));
    }

    @Transactional(readOnly = true)
    public TimelineResponse timeline(UUID id) {
        return TimelineResponse.from(find(id));
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> findByStatus(OrderStatus status, int olderThanMinutes) {
        Instant cutoff = Instant.now().minus(Duration.ofMinutes(olderThanMinutes));
        return orders.findByStatusAndCreatedAtBefore(status, cutoff).stream().map(OrderResponse::from).toList();
    }

    private Order find(UUID id) {
        return orders.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order " + id + " not found"));
    }
}

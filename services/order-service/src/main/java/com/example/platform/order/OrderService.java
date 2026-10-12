package com.example.platform.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.UUID;

@Service
public class OrderService {

    static final String ORDER_CREATED = "order.created";

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orders;
    private final KafkaTemplate<String, Object> kafka;

    OrderService(OrderRepository orders, KafkaTemplate<String, Object> kafka) {
        this.orders = orders;
        this.kafka = kafka;
    }

    // Deliberately NOT @Transactional: save() runs in its own transaction and commits when it returns,
    // so we only publish order.created for an order that really exists in the database.
    // ponytail: if Kafka is down after the commit, the order stays PENDING with no event (dual-write problem,
    // explored in Phase 6; transactional outbox is the fix).
    public OrderResponse create(CreateOrderRequest request) {
        Order order = new Order(request.customerId());
        request.items().forEach(item -> order.addItem(item.sku(), item.quantity()));
        orders.save(order);

        var event = EventEnvelope.of(ORDER_CREATED, order.getId(), OrderCreatedPayload.from(order));
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

    @Transactional(readOnly = true)
    public OrderResponse get(UUID id) {
        return orders.findById(id)
                .map(OrderResponse::from)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order " + id + " not found"));
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> findByStatus(OrderStatus status) {
        return orders.findByStatus(status).stream().map(OrderResponse::from).toList();
    }
}

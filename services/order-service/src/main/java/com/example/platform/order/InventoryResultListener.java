package com.example.platform.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.TimeUnit;

/** Last step of the saga: turn inventory's answer into CONFIRMED or CANCELLED and announce it. */
@Component
class InventoryResultListener {

    static final String INVENTORY_RESERVED = "inventory.reserved";
    static final String INVENTORY_REJECTED = "inventory.rejected";
    static final String ORDER_CONFIRMED = "order.confirmed";
    static final String ORDER_CANCELLED = "order.cancelled";

    /** Both inventory events fit this shape: "reason" is only set on inventory.rejected. */
    record InventoryResult(String reason) {
    }

    record OrderCancelledPayload(String customerId, String reason) {
    }

    private static final Logger log = LoggerFactory.getLogger(InventoryResultListener.class);

    private final OrderRepository orders;
    private final ProcessedEventRepository processedEvents;
    private final KafkaTemplate<String, Object> kafka;

    InventoryResultListener(OrderRepository orders, ProcessedEventRepository processedEvents, KafkaTemplate<String, Object> kafka) {
        this.orders = orders;
        this.processedEvents = processedEvents;
        this.kafka = kafka;
    }

    // Same steps as inventory's ReservationListener: dedupe -> change state + mark processed -> flush -> publish.
    @KafkaListener(topics = {INVENTORY_RESERVED, INVENTORY_REJECTED})
    @Transactional
    public void onInventoryResult(EventEnvelope<InventoryResult> event) throws Exception {
        if (processedEvents.existsById(event.eventId())) {
            log.info("Duplicate event {} for order {}, skipping", event.eventId(), event.orderId());
            return;
        }
        processedEvents.save(new ProcessedEvent(event.eventId()));

        Order order = orders.findById(event.orderId()).orElse(null);
        if (order == null || order.getStatus() != OrderStatus.PENDING) {
            // A different event already decided this order (or it doesn't exist). Nothing to do, but don't retry forever.
            log.warn("Ignoring {} for order {}: order is {}", event.eventType(), event.orderId(),
                    order == null ? "unknown" : order.getStatus());
            return;
        }

        boolean reserved = INVENTORY_RESERVED.equals(event.eventType());
        String reason = event.payload().reason();
        order.complete(reserved ? OrderStatus.CONFIRMED : OrderStatus.CANCELLED, event.eventId(), event.eventType(), reason);
        orders.flush();

        EventEnvelope<?> result = reserved
                ? EventEnvelope.of(ORDER_CONFIRMED, order.getId(), OrderPayload.from(order))
                : EventEnvelope.of(ORDER_CANCELLED, order.getId(), new OrderCancelledPayload(order.getCustomerId(), reason));
        kafka.send(result.eventType(), order.getId().toString(), result).get(10, TimeUnit.SECONDS);

        log.info("Order {} -> {}", order.getId(), order.getStatus());
    }
}

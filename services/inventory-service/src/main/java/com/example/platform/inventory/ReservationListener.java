package com.example.platform.inventory;

import com.example.platform.inventory.Events.EventEnvelope;
import com.example.platform.inventory.Events.InventoryRejected;
import com.example.platform.inventory.Events.InventoryReserved;
import com.example.platform.inventory.Events.OrderCreated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
class ReservationListener {

    private static final Logger log = LoggerFactory.getLogger(ReservationListener.class);

    private final StockRepository stocks;
    private final ProcessedEventRepository processedEvents;
    private final KafkaTemplate<String, Object> kafka;

    ReservationListener(StockRepository stocks, ProcessedEventRepository processedEvents, KafkaTemplate<String, Object> kafka) {
        this.stocks = stocks;
        this.processedEvents = processedEvents;
        this.kafka = kafka;
    }

    // Like a @PostMapping method, but triggered by a Kafka record instead of an HTTP request.
    // Order of steps matters:
    //   1. skip if already processed      (idempotent consumer)
    //   2. reserve stock + record eventId  (same DB transaction: both happen or neither)
    //   3. flush                           (optimistic-lock conflicts fail HERE, before we publish anything)
    //   4. publish result and wait for ack (if this fails, the exception rolls back 1-3 and Kafka redelivers)
    //   5. method returns -> DB commit -> container commits the offset
    // Remaining gap: DB commit fails after step 4 -> result published, stock not reserved -> redelivery
    // publishes a second result. A duplicate is safe (order-service is idempotent too); a lost result is not.
    @KafkaListener(topics = Events.ORDER_CREATED)
    @Transactional
    public void onOrderCreated(EventEnvelope<OrderCreated> event) throws Exception {
        if (processedEvents.existsById(event.eventId())) {
            log.info("Duplicate event {} for order {}, skipping", event.eventId(), event.orderId());
            return;
        }

        // Same SKU twice in one order counts as one combined request.
        Map<String, Integer> requested = event.payload().items().stream()
                .collect(Collectors.toMap(Events.Item::sku, Events.Item::quantity, Integer::sum, TreeMap::new));
        Map<String, Stock> stockBySku = stocks.findAllById(requested.keySet()).stream()
                .collect(Collectors.toMap(Stock::getSku, Function.identity()));

        // All-or-nothing: check every item before changing any stock.
        String rejection = findShortage(requested, stockBySku);
        if (rejection == null) {
            requested.forEach((sku, quantity) -> stockBySku.get(sku).reserve(quantity));
        }

        processedEvents.save(new ProcessedEvent(event.eventId()));
        stocks.flush();

        EventEnvelope<?> result = rejection == null
                ? EventEnvelope.of(Events.INVENTORY_RESERVED, event.orderId(), new InventoryReserved(event.payload().items()))
                : EventEnvelope.of(Events.INVENTORY_REJECTED, event.orderId(), new InventoryRejected(rejection));
        kafka.send(result.eventType(), event.orderId().toString(), result).get(10, TimeUnit.SECONDS);

        log.info("Order {} -> {}{}", event.orderId(), result.eventType(), rejection == null ? "" : " (" + rejection + ")");
    }

    private static String findShortage(Map<String, Integer> requested, Map<String, Stock> stockBySku) {
        for (var entry : requested.entrySet()) {
            Stock stock = stockBySku.get(entry.getKey());
            if (stock == null) {
                return "Unknown SKU " + entry.getKey();
            }
            if (stock.getAvailable() < entry.getValue()) {
                return "Not enough stock for %s (requested %d, available %d)"
                        .formatted(entry.getKey(), entry.getValue(), stock.getAvailable());
            }
        }
        return null;
    }
}

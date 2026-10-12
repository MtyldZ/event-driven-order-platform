package com.example.platform.order;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderResponse(UUID id, String customerId, OrderStatus status, Instant createdAt, List<Item> items) {

    public record Item(String sku, int quantity) {
    }

    static OrderResponse from(Order order) {
        List<Item> items = order.getItems().stream()
                .map(i -> new Item(i.getSku(), i.getQuantity()))
                .toList();
        return new OrderResponse(order.getId(), order.getCustomerId(), order.getStatus(), order.getCreatedAt(), items);
    }
}

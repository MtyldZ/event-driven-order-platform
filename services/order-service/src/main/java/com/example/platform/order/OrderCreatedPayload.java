package com.example.platform.order;

import java.util.List;

// Kept separate from the REST DTOs: the event contract and the API can evolve independently.
public record OrderCreatedPayload(String customerId, List<Item> items) {

    public record Item(String sku, int quantity) {
    }

    static OrderCreatedPayload from(Order order) {
        List<Item> items = order.getItems().stream()
                .map(i -> new Item(i.getSku(), i.getQuantity()))
                .toList();
        return new OrderCreatedPayload(order.getCustomerId(), items);
    }
}

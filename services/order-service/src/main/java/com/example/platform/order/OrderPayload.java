package com.example.platform.order;

import java.util.List;

// Payload of order.created and order.confirmed.
// Kept separate from the REST DTOs: the event contract and the API can evolve independently.
public record OrderPayload(String customerId, List<Item> items) {

    public record Item(String sku, int quantity) {
    }

    static OrderPayload from(Order order) {
        List<Item> items = order.getItems().stream()
                .map(i -> new Item(i.getSku(), i.getQuantity()))
                .toList();
        return new OrderPayload(order.getCustomerId(), items);
    }
}

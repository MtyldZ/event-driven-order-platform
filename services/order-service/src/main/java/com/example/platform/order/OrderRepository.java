package com.example.platform.order;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<Order, UUID> {

    // Without this: 1 query for orders + 1 query per order for its items (N+1).
    // With it: one query, orders LEFT JOIN order_items.
    @EntityGraph(attributePaths = "items")
    List<Order> findByStatus(OrderStatus status);
}

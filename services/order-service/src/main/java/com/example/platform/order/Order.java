package com.example.platform.order;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders") // "order" is a reserved word in SQL
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String customerId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    @Column(nullable = false)
    private Instant createdAt;

    @Version
    private long version;

    // Inverse side: the foreign key lives in order_items.order_id (owned by OrderItem.order).
    // @OneToMany is LAZY by default: items are loaded only when first accessed.
    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderItem> items = new ArrayList<>();

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL)
    @OrderBy("occurredAt ASC, id ASC")
    private List<OrderStatusHistory> history = new ArrayList<>();

    protected Order() {
        // for JPA
    }

    /** @param createdEventId id of the order.created event that will announce this order */
    public Order(String customerId, UUID createdEventId) {
        this.customerId = customerId;
        this.status = OrderStatus.PENDING;
        this.createdAt = Instant.now();
        history.add(new OrderStatusHistory(this, OrderStatus.PENDING, createdEventId, OrderService.ORDER_CREATED, null));
    }

    public void addItem(String sku, int quantity) {
        items.add(new OrderItem(this, sku, quantity));
    }

    /** The only allowed transitions are PENDING -> CONFIRMED and PENDING -> CANCELLED. */
    public void complete(OrderStatus newStatus, UUID eventId, String eventType, String reason) {
        if (status != OrderStatus.PENDING || newStatus == OrderStatus.PENDING) {
            throw new IllegalStateException("Order " + id + " cannot go from " + status + " to " + newStatus);
        }
        status = newStatus;
        history.add(new OrderStatusHistory(this, newStatus, eventId, eventType, reason));
    }

    public UUID getId() { return id; }
    public String getCustomerId() { return customerId; }
    public OrderStatus getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
    public List<OrderItem> getItems() { return items; }
    public List<OrderStatusHistory> getHistory() { return history; }
}

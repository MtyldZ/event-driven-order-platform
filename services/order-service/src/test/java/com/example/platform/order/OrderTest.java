package com.example.platform.order;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderTest {

    @Test
    void newOrderIsPendingWithOneHistoryEntry() {
        UUID createdEventId = UUID.randomUUID();

        Order order = new Order("cust-1", createdEventId);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getHistory()).singleElement().satisfies(h -> {
            assertThat(h.getStatus()).isEqualTo(OrderStatus.PENDING);
            assertThat(h.getEventId()).isEqualTo(createdEventId);
        });
    }

    @Test
    void pendingOrderCanBeConfirmed() {
        Order order = new Order("cust-1", UUID.randomUUID());

        order.complete(OrderStatus.CONFIRMED, UUID.randomUUID(), "inventory.reserved", null);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(order.getHistory()).extracting(OrderStatusHistory::getStatus)
                .containsExactly(OrderStatus.PENDING, OrderStatus.CONFIRMED);
    }

    @Test
    void decidedOrderCannotChangeAgain() {
        Order order = new Order("cust-1", UUID.randomUUID());
        order.complete(OrderStatus.CONFIRMED, UUID.randomUUID(), "inventory.reserved", null);

        assertThatThrownBy(() -> order.complete(OrderStatus.CANCELLED, UUID.randomUUID(), "inventory.rejected", "late"))
                .isInstanceOf(IllegalStateException.class);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    }

    @Test
    void cannotCompleteToPending() {
        Order order = new Order("cust-1", UUID.randomUUID());

        assertThatThrownBy(() -> order.complete(OrderStatus.PENDING, UUID.randomUUID(), "x", null))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void pendingOrderCanBeCancelledWithAReason() {
        Order order = new Order("cust-1", UUID.randomUUID());
        UUID rejectedEventId = UUID.randomUUID();
        String reason = "No stock";
        order.complete(OrderStatus.CANCELLED, rejectedEventId, "inventory.rejected", reason);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getHistory()).extracting(OrderStatusHistory::getStatus).containsExactly(OrderStatus.PENDING, OrderStatus.CANCELLED);
        assertThat(order.getHistory().getLast()).satisfies(h -> {
            assertThat(h.getEventId()).isEqualTo(rejectedEventId);
            assertThat(h.getReason()).isEqualTo(reason);
        });
    }
}

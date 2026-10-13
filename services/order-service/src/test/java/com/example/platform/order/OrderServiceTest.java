package com.example.platform.order;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit test of the pre-check logic: every collaborator is a Mockito mock, no Spring context. */
class OrderServiceTest {

    OrderRepository orders = mock(OrderRepository.class);
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, Object> kafka = mock(KafkaTemplate.class);
    InventoryClient inventory = mock(InventoryClient.class);
    OrderService service = new OrderService(orders, kafka, inventory);

    @BeforeEach
    void setUp() {
        // Pretend to be the database: assign the id Hibernate would generate.
        when(orders.save(any())).thenAnswer(call -> {
            Order order = call.getArgument(0); // typed variable: otherwise Java picks setField(Class, ...) and fails
            ReflectionTestUtils.setField(order, "id", UUID.randomUUID());
            return order;
        });
        when(kafka.send(anyString(), anyString(), any())).thenReturn(new CompletableFuture<>());
    }

    @Test
    void rejectsWith422WhenInventorySaysNotEnough() {
        when(inventory.availableStock("SKU-1")).thenReturn(Optional.of(1));

        assertThatThrownBy(() -> service.create(request("SKU-1", 2)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        e -> assertThat(e.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT));
        verify(orders, never()).save(any());
        verify(kafka, never()).send(anyString(), anyString(), any());
    }

    @Test
    void sumsQuantitiesOfTheSameSkuBeforeChecking() {
        when(inventory.availableStock("SKU-1")).thenReturn(Optional.of(3));

        var twoLines = new CreateOrderRequest("cust-1", List.of(
                new CreateOrderRequest.Item("SKU-1", 2), new CreateOrderRequest.Item("SKU-1", 2)));

        assertThatThrownBy(() -> service.create(twoLines)).isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void acceptsAsPendingWhenInventoryCannotAnswer() {
        when(inventory.availableStock("SKU-1")).thenReturn(Optional.empty()); // what the fallback returns

        OrderResponse response = service.create(request("SKU-1", 2));

        assertThat(response.status()).isEqualTo(OrderStatus.PENDING);
        verify(kafka).send(eq("order.created"), eq(response.id().toString()), any());
    }

    private static CreateOrderRequest request(String sku, int quantity) {
        return new CreateOrderRequest("cust-1", List.of(new CreateOrderRequest.Item(sku, quantity)));
    }
}

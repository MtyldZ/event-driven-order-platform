package com.example.platform.order;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


/** Web slice: only Spring MVC and ApiExceptionHandler start; OrderService is a mock (no database, no Kafka). */
@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    MockMvcTester mvc;
    @MockitoBean
    OrderService orderService;

    @Test
    void createReturns201WithLocation() {
        var id = UUID.randomUUID();
        var response = new OrderResponse(id, "cust-1", OrderStatus.PENDING, Instant.now(), List.of());
        when(orderService.create(any())).thenReturn(response);

        assertThat(mvc.post().uri("/orders")
            .contentType(MediaType.APPLICATION_JSON)
            .content("""
                {"customerId":"cust-1","items":[{"sku":"sku-x","quantity":2}]}
                """
            ))
            .hasHeader("Location", "/orders/" + id)
            .hasStatus(HttpStatus.CREATED)
            .bodyJson().extractingPath("$.status").isEqualTo("PENDING");

        ArgumentCaptor<CreateOrderRequest> request = ArgumentCaptor.forClass(CreateOrderRequest.class);
        verify(orderService).create(request.capture());
        assertThat(request.getValue().customerId()).isEqualTo("cust-1");
        assertThat(request.getValue().items()).containsExactly(new CreateOrderRequest.Item("sku-x", 2));
    }

    @Test
    void invalidBodyReturns422WithFieldErrors() {
        var result = mvc.post().uri("/orders")
            .contentType(MediaType.APPLICATION_JSON)
            .content("""
                {"customerId":"","items":[]}
                """
            ).exchange();
        assertThat(result).hasStatus(HttpStatus.UNPROCESSABLE_CONTENT);
        assertThat(result).bodyJson().extractingPath("$.errors").asMap().containsOnlyKeys("customerId", "items");

        verify(orderService, never()).create(any());
    }

    @Test
    void unparsableJsonReturns400() {
        assertThat(mvc.post().uri("/orders")
            .contentType(MediaType.APPLICATION_JSON)
            .content("""
                {"customerId":
                """
            ))
            .hasStatus(HttpStatus.BAD_REQUEST);

        verify(orderService, never()).create(any());
    }
}

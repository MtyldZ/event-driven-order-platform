package com.example.platform.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
class OrderController {

    private final OrderService service;

    OrderController(OrderService service) {
        this.service = service;
    }

    @PostMapping
    ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
        OrderResponse created = service.create(request);
        return ResponseEntity.created(URI.create("/orders/" + created.id())).body(created);
    }

    @GetMapping("/{id}")
    OrderResponse get(@PathVariable UUID id) {
        return service.get(id);
    }

    @GetMapping("/{id}/timeline")
    TimelineResponse timeline(@PathVariable UUID id) {
        return service.timeline(id);
    }

    // GET /orders?status=PENDING&olderThanMinutes=5 -> orders that may be stuck
    @GetMapping
    List<OrderResponse> list(@RequestParam OrderStatus status,
                             @RequestParam(defaultValue = "0") @PositiveOrZero int olderThanMinutes) {
        return service.findByStatus(status, olderThanMinutes);
    }
}

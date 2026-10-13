package com.example.platform.order;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Optional;

/** Synchronous stock pre-check against inventory-service: GET /stock/{sku}. */
@Component
class InventoryClient {

    record StockLevel(String sku, int available) {
    }

    private static final Logger log = LoggerFactory.getLogger(InventoryClient.class);

    private final RestClient rest;

    InventoryClient(RestClient.Builder builder, @Value("${inventory.base-url}") String baseUrl,
                    CircuitBreakerRegistry circuitBreakers) {
        // Timeouts: never let a slow inventory-service hold our HTTP threads for long.
        var requestFactory = new JdkClientHttpRequestFactory(
                HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500)).build());
        requestFactory.setReadTimeout(Duration.ofSeconds(1));
        this.rest = builder.baseUrl(baseUrl).requestFactory(requestFactory).build();

        // Resilience4j doesn't log state changes by itself; make CLOSED -> OPEN -> HALF_OPEN visible.
        circuitBreakers.circuitBreaker("inventory").getEventPublisher()
                .onStateTransition(e -> log.warn("Circuit breaker 'inventory': {}", e.getStateTransition()));
    }

    /**
     * @return units available (0 for an unknown SKU), or empty if inventory-service could not answer.
     *
     * Resilience4j applies Retry OUTSIDE CircuitBreaker: Retry(CircuitBreaker(call)).
     * Every attempt is counted by the breaker; when it is open, it fails fast with CallNotPermittedException,
     * which Retry is configured to ignore (no point retrying a call that is not permitted).
     * The fallback sits on the outermost layer (Retry). On the CircuitBreaker it would swallow the
     * exception before Retry ever saw it, and nothing would be retried.
     */
    @Retry(name = "inventory", fallbackMethod = "inventoryUnavailable")
    @CircuitBreaker(name = "inventory")
    public Optional<Integer> availableStock(String sku) {
        StockLevel level = rest.get().uri("/stock/{sku}", sku).exchange((request, response) -> {
            if (response.getStatusCode().value() == 404) {
                return new StockLevel(sku, 0); // a valid answer, not a failure: must not open the breaker
            }
            if (response.getStatusCode().isError()) {
                throw new IllegalStateException("inventory-service returned " + response.getStatusCode());
            }
            return response.bodyTo(StockLevel.class);
        });
        return Optional.of(level.available());
    }

    // Decision from the brief: inventory unreachable -> accept the order as PENDING, the async saga decides.
    Optional<Integer> inventoryUnavailable(String sku, Throwable cause) {
        log.warn("Stock pre-check for {} skipped: {}", sku, cause.toString());
        return Optional.empty();
    }
}

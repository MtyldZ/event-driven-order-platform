package com.example.platform.inventory;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Plain unit tests: no Spring, no database, run in milliseconds. */
class StockTest {

    @Test
    void reserveReducesAvailableStock() {
        Stock stock = new Stock("SKU-1", 5);

        stock.reserve(2);

        assertThat(stock.getAvailable()).isEqualTo(3);
    }

    @Test
    void reserveMoreThanAvailableFailsAndChangesNothing() {
        Stock stock = new Stock("SKU-1", 1);

        assertThatThrownBy(() -> stock.reserve(2)).isInstanceOf(IllegalStateException.class);
        assertThat(stock.getAvailable()).isEqualTo(1);
    }

    @Test
    void findShortageReturnsNullWhenEverythingIsAvailable() {
        var stock = Map.of("A", new Stock("A", 2), "B", new Stock("B", 1));

        assertThat(ReservationListener.findShortage(Map.of("A", 2, "B", 1), stock)).isNull();
    }

    @Test
    void findShortageReportsTheShortItem() {
        var stock = Map.of("A", new Stock("A", 2), "B", new Stock("B", 1));

        assertThat(ReservationListener.findShortage(Map.of("A", 1, "B", 3), stock))
                .isEqualTo("Not enough stock for B (requested 3, available 1)");
    }

    @Test
    void findShortageReportsUnknownSku() {
        assertThat(ReservationListener.findShortage(Map.of("X", 1), Map.of())).isEqualTo("Unknown SKU X");
    }
}

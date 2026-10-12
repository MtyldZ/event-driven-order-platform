package com.example.platform.inventory;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
class StockController {

    record StockResponse(String sku, int available) {
    }

    private final StockRepository stocks;

    StockController(StockRepository stocks) {
        this.stocks = stocks;
    }

    @GetMapping("/stock/{sku}")
    StockResponse get(@PathVariable String sku) {
        return stocks.findById(sku)
                .map(s -> new StockResponse(s.getSku(), s.getAvailable()))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown SKU " + sku));
    }
}

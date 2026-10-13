package com.example.platform.inventory;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
class StockController {

    private final StockService stockService;

    StockController(StockService stockService) {
        this.stockService = stockService;
    }

    @GetMapping("/stock/{sku}")
    StockService.StockResponse get(@PathVariable String sku) {
        return stockService.get(sku);
    }
}

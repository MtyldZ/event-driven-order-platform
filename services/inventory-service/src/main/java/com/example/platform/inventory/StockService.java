package com.example.platform.inventory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
class StockService {

    static final String CACHE = "stock";

    record StockResponse(String sku, int available) {
    }

    private static final Logger log = LoggerFactory.getLogger(StockService.class);

    private final StockRepository stocks;

    StockService(StockRepository stocks) {
        this.stocks = stocks;
    }

    // Cache-aside: Spring looks in Redis under key "stock::<sku>" first; only on a miss does this method run,
    // and its result is stored. Exceptions (unknown SKU) are not cached.
    @Cacheable(CACHE)
    public StockResponse get(String sku) {
        log.info("Cache miss for {}, reading database", sku);
        return stocks.findById(sku)
                .map(s -> new StockResponse(s.getSku(), s.getAvailable()))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown SKU " + sku));
    }
}

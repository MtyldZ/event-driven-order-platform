package com.example.platform.inventory;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "stock")
public class Stock {

    @Id
    private String sku;

    private int available;

    // Hibernate adds "WHERE version = ?" to every UPDATE and increments it.
    // If another transaction changed the row first, 0 rows match -> OptimisticLockException.
    @Version
    private long version;

    protected Stock() {
        // for JPA
    }

    public void reserve(int quantity) {
        if (quantity > available) {
            throw new IllegalStateException("Not enough stock for " + sku);
        }
        available -= quantity;
    }

    public String getSku() { return sku; }
    public int getAvailable() { return available; }
}

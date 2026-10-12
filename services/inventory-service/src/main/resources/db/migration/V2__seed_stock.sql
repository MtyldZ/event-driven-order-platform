INSERT INTO stock (sku, available) VALUES
    ('SKU-1', 100),
    ('SKU-2', 50),
    ('SKU-3', 5),  -- low stock: used to show competing orders
    ('SKU-4', 0);  -- out of stock: always rejected

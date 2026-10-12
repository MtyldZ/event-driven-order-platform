-- Runs once, on first start with an empty data volume.
-- One database per service: each service only ever connects to its own.
CREATE DATABASE orders_db;
CREATE DATABASE inventory_db;
CREATE DATABASE notifications_db;

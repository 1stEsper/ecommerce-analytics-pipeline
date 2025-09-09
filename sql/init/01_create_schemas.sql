-- Create schemas for data organization
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS streaming;

-- Raw data tables for Olist dataset
CREATE TABLE IF NOT EXISTS raw_data.customers (
    customer_id VARCHAR(255) PRIMARY KEY,
    customer_unique_id VARCHAR(255),
    customer_zip_code_prefix INTEGER,
    customer_city VARCHAR(255),
    customer_state VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS raw_data.orders (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES raw_data.customers(customer_id)
);

CREATE TABLE IF NOT EXISTS raw_data.order_items (
    order_id VARCHAR(255),
    order_item_id INTEGER,
    product_id VARCHAR(255),
    seller_id VARCHAR(255),
    shipping_limit_date TIMESTAMP,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES raw_data.orders(order_id)
);

CREATE TABLE IF NOT EXISTS raw_data.products (
    product_id VARCHAR(255) PRIMARY KEY,
    product_category_name VARCHAR(255),
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

-- Staging tables for processed data
CREATE TABLE IF NOT EXISTS staging.customer_metrics (
    customer_id VARCHAR(255) PRIMARY KEY,
    total_orders INTEGER,
    total_spent DECIMAL(15,2),
    avg_order_value DECIMAL(12,2),
    first_order_date DATE,
    last_order_date DATE,
    customer_lifetime_days INTEGER,
    favorite_category VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Analytics tables for business intelligence
CREATE TABLE IF NOT EXISTS analytics.daily_sales (
    sale_date DATE PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(15,2),
    unique_customers INTEGER,
    avg_order_value DECIMAL(12,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.product_performance (
    product_id VARCHAR(255) PRIMARY KEY,
    product_category VARCHAR(255),
    total_quantity_sold INTEGER,
    total_revenue DECIMAL(15,2),
    avg_price DECIMAL(10,2),
    total_orders INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Streaming tables for real-time data
CREATE TABLE IF NOT EXISTS streaming.live_orders (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    product_id VARCHAR(255),
    order_value DECIMAL(10,2),
    order_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON raw_data.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON raw_data.orders(order_status);
CREATE INDEX IF NOT EXISTS idx_orders_timestamp ON raw_data.orders(order_purchase_timestamp);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON raw_data.order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_daily_sales_date ON analytics.daily_sales(sale_date);

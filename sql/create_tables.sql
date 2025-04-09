-- This script creates the dimension and fact tables in the data warehouse redshift

-- Customer dimension table
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR(35) PRIMARY KEY,
    customer_zip_code_prefix INT,
    customer_city VARCHAR(20),
    customer_state VARCHAR(2),
    geolocation_lat FLOAT8,
    geolocation_lng FLOAT8
);

-- Seller dimension table
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id VARCHAR(35) PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(20),
    seller_state VARCHAR(2),
    geolocation_lat FLOAT8,
    geolocation_lng FLOAT8
);

-- Product dimension table
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(35) PRIMARY KEY,
    product_category_name_english VARCHAR(25)
);

-- Items dimension table
CREATE TABLE IF NOT EXISTS dim_items (
    order_id VARCHAR(35),
    product_id VARCHAR(35),
    seller_id VARCHAR(35),
    price FLOAT8,
    freight_value FLOAT8,
    review_id VARCHAR(35),
    review_score INT,
    PRIMARY KEY (order_id, product_id, seller_id)
);

-- Payments dimension table
CREATE TABLE IF NOT EXISTS dim_payments (
    order_id VARCHAR(35) PRIMARY KEY,
    payment_type VARCHAR(20),
    payment_installments INT,
    payment_value FLOAT4
);


-- Orders fact table
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id VARCHAR(35) PRIMARY KEY,
    customer_id VARCHAR(35),
    order_purchase_timestamp TIMESTAMP,
    customer_city VARCHAR(20),
    product_id VARCHAR(35),
    seller_id VARCHAR(35),
    price FLOAT8,
    freight_value FLOAT8,
    seller_city VARCHAR(20),
    product_category_name_english VARCHAR(25),
    payment_type VARCHAR(20),
    payment_installments INT,
    payment_value FLOAT4
);
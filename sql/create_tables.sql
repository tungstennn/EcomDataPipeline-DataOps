-- This script creates the staging, dimension and fact tables in the data warehouse redshift


-- Customer dimension table
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_zip_code_prefix INT,
    customer_city VARCHAR,
    customer_state VARCHAR,
    geolocation_lat FLOAT8,
    geolocation_lng FLOAT8
);

-- Seller dimension table
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id VARCHAR PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR,
    seller_state VARCHAR,
    geolocation_lat FLOAT8,
    geolocation_lng FLOAT8
);

-- Product dimension table
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR PRIMARY KEY,
    product_category_name_english VARCHAR
);

-- Payments dimension table
CREATE TABLE IF NOT EXISTS dim_payments (
    order_id VARCHAR PRIMARY KEY,
    payment_type VARCHAR,
    payment_installments INT,
    payment_value FLOAT4
);

-- Order Items fact table
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_id VARCHAR,
    product_id VARCHAR,
    seller_id VARCHAR,
    price FLOAT8,
    freight_value FLOAT8,
    review_id VARCHAR,
    review_score INT,
    PRIMARY KEY (order_id, product_id, seller_id)
);

-- Orders fact table
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    order_purchase_timestamp TIMESTAMP
);


-- Staging Tables (raw CSV format)


CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id VARCHAR,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix VARCHAR,
    customer_city VARCHAR,
    customer_state VARCHAR
);

CREATE TABLE IF NOT EXISTS stg_geolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat VARCHAR,
    geolocation_lng VARCHAR,
    geolocation_city VARCHAR,
    geolocation_state VARCHAR
);

CREATE TABLE IF NOT EXISTS stg_sellers (
    seller_id VARCHAR,
    seller_zip_code_prefix INT,
    seller_city VARCHAR,
    seller_state VARCHAR
);

CREATE TABLE IF NOT EXISTS stg_products (
    product_id VARCHAR,
    product_category_name VARCHAR,
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g VARCHAR,
    product_length_cm VARCHAR,
    product_height_cm VARCHAR,
    product_width_cm VARCHAR
);

CREATE TABLE IF NOT EXISTS stg_product_category_name_translation (
    product_category_name VARCHAR,
    product_category_name_english VARCHAR
);

CREATE TABLE IF NOT EXISTS stg_order_items (
    order_id VARCHAR,
    order_item_id INT,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date VARCHAR,
    price VARCHAR,
    freight_value VARCHAR
);

CREATE TABLE IF NOT EXISTS stg_order_payments (
    order_id VARCHAR,
    payment_sequential INT,
    payment_type VARCHAR,
    payment_installments INT,
    payment_value VARCHAR
);

CREATE TABLE IF NOT EXISTS stg_order_reviews (
    review_id VARCHAR,
    order_id VARCHAR,
    review_score INT,
    review_comment_title VARCHAR,
    review_comment_message VARCHAR(10000),
    review_creation_date VARCHAR,   -- might change to varchar
    review_answer_timestamp VARCHAR
);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id VARCHAR,
    customer_id VARCHAR,
    order_status VARCHAR,
    order_purchase_timestamp VARCHAR,
    order_approved_at VARCHAR,
    order_delivered_carrier_date VARCHAR,
    order_delivered_customer_date VARCHAR,
    order_estimated_delivery_date VARCHAR
);







-- SELECT * FROM sys_load_error_detail
-- ORDER BY start_time DESC
-- LIMIT 10;


-- DROP TABLE IF EXISTS stg_customers;
-- DROP TABLE IF EXISTS stg_orders;
-- DROP TABLE IF EXISTS stg_sellers;
-- DROP TABLE IF EXISTS stg_order_reviews;
-- DROP TABLE IF EXISTS stg_order_payments;
-- DROP TABLE IF EXISTS stg_order_items;
-- DROP TABLE IF EXISTS stg_product_category_name_translation;
-- DROP TABLE IF EXISTS stg_products;
-- DROP TABLE IF EXISTS stg_geolocation;
-- DROP TABLE IF EXISTS dim_customers;
-- DROP TABLE IF EXISTS dim_sellers;
-- DROP TABLE IF EXISTS dim_products;
-- DROP TABLE IF EXISTS dim_items;
-- DROP TABLE IF EXISTS dim_payments;
-- DROP TABLE IF EXISTS fact_orders;


-- Customer dimension table
DROP TABLE IF EXISTS dim_customers;

CREATE TABLE dim_customers AS
WITH avg_geo AS (
    SELECT
        geolocation_zip_code_prefix,
        AVG(CAST(geolocation_lat AS FLOAT8)) AS geolocation_lat,
        AVG(CAST(geolocation_lng AS FLOAT8)) AS geolocation_lng
    FROM stg_geolocation
    WHERE geolocation_zip_code_prefix IS NOT NULL
    GROUP BY geolocation_zip_code_prefix
)
SELECT
    c.customer_id,
    CAST(c.customer_zip_code_prefix AS INT) AS customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    g.geolocation_lat,
    g.geolocation_lng
FROM stg_customers c
LEFT JOIN avg_geo g
    ON CAST(c.customer_zip_code_prefix AS INT) = g.geolocation_zip_code_prefix;


-- Seller dimension table
DROP TABLE IF EXISTS dim_sellers;

CREATE TABLE dim_sellers AS
WITH avg_geo AS (
    SELECT
        geolocation_zip_code_prefix,
        AVG(CAST(geolocation_lat AS FLOAT8)) AS geolocation_lat,
        AVG(CAST(geolocation_lng AS FLOAT8)) AS geolocation_lng
    FROM stg_geolocation
    WHERE geolocation_zip_code_prefix IS NOT NULL
    GROUP BY geolocation_zip_code_prefix
)
SELECT
    s.seller_id,
    CAST(s.seller_zip_code_prefix AS INT) AS seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    g.geolocation_lat,
    g.geolocation_lng
FROM stg_sellers s
LEFT JOIN avg_geo g
    ON CAST(s.seller_zip_code_prefix AS INT) = g.geolocation_zip_code_prefix;


-- Product dimension table
DROP TABLE IF EXISTS dim_products;

CREATE TABLE dim_products AS
SELECT
    p.product_id,
    c.product_category_name_english
FROM stg_products p
LEFT JOIN stg_product_category_name_translation c
    ON p.product_category_name = c.product_category_name;


-- Items dimension table
DROP TABLE IF EXISTS dim_items;

CREATE TABLE dim_items AS
SELECT
    i.order_id,
    i.product_id,
    i.seller_id,
    CAST(i.price AS FLOAT4) AS price,
    CAST(i.freight_value AS FLOAT4) AS freight_value,
    r.review_id,
    r.review_score
FROM stg_order_items i
LEFT JOIN stg_order_reviews r
    ON i.order_id = r.order_id;


-- Payments dimension table
DROP TABLE IF EXISTS dim_payments;

CREATE TABLE dim_payments AS
SELECT
    p.order_id,
    p.payment_type,
    p.payment_installments,
    CAST(p.payment_value AS FLOAT4) AS payment_value
FROM stg_order_payments p;


-- Orders fact table
DROP TABLE IF EXISTS fact_orders;

CREATE TABLE fact_orders AS
SELECT
    o.order_id,
    c.customer_id,
    o.order_purchase_timestamp,
    c.customer_city,
    i.product_id,
    i.seller_id,
    i.price,
    i.freight_value,
    s.seller_city,
    p.product_category_name_english,
    pay.payment_type,
    pay.payment_installments,
    pay.payment_value
FROM stg_orders o
JOIN dim_customers c
    ON o.customer_id = c.customer_id
LEFT JOIN dim_items i
    ON o.order_id = i.order_id
LEFT JOIN dim_sellers s
    ON i.seller_id = s.seller_id
LEFT JOIN dim_products p
    ON i.product_id = p.product_id
LEFT JOIN dim_payments pay
    ON o.order_id = pay.order_id;
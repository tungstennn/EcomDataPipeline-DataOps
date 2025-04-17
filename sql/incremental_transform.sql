-- Customer Dimension Table Incremental Load

INSERT INTO dim_customers (
    customer_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    geolocation_lat,
    geolocation_lng
)
SELECT
    c.customer_id,
    CAST(c.customer_zip_code_prefix AS INT),
    c.customer_city,
    c.customer_state,
    g.avg_lat,
    g.avg_lng
FROM stg_customers c
LEFT JOIN (
    SELECT
        geolocation_zip_code_prefix,
        AVG(CAST(geolocation_lat AS FLOAT8)) AS avg_lat,
        AVG(CAST(geolocation_lng AS FLOAT8)) AS avg_lng
    FROM stg_geolocation
    WHERE geolocation_zip_code_prefix IS NOT NULL
    GROUP BY geolocation_zip_code_prefix
) g ON CAST(c.customer_zip_code_prefix AS INT) = g.geolocation_zip_code_prefix
WHERE c.customer_id NOT IN (SELECT customer_id FROM dim_customers);

-- Seller Dimension Table Incremental Load

INSERT INTO dim_sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    geolocation_lat,
    geolocation_lng
)
SELECT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    g.avg_lat,
    g.avg_lng
FROM stg_sellers s
LEFT JOIN (
    SELECT
        geolocation_zip_code_prefix,
        AVG(CAST(geolocation_lat AS FLOAT8)) AS avg_lat,
        AVG(CAST(geolocation_lng AS FLOAT8)) AS avg_lng
    FROM stg_geolocation
    WHERE geolocation_zip_code_prefix IS NOT NULL
    GROUP BY geolocation_zip_code_prefix
) g ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
WHERE s.seller_id NOT IN (SELECT seller_id FROM dim_sellers);

-- Product Dimension Table Incremental Load

INSERT INTO dim_products (
    product_id,
    product_category_name_english
)
SELECT
    p.product_id,
    t.product_category_name_english
FROM stg_products p
LEFT JOIN stg_product_category_name_translation t
    ON p.product_category_name = t.product_category_name
WHERE p.product_id NOT IN (SELECT product_id FROM dim_products);

-- Items Dimension Table Incremental Load

INSERT INTO dim_items (
    order_id,
    product_id,
    seller_id,
    price,
    freight_value,
    review_id,
    review_score
)
SELECT
    i.order_id,
    i.product_id,
    i.seller_id,
    CAST(i.price AS FLOAT4),
    CAST(i.freight_value AS FLOAT4),
    r.review_id,
    r.review_score
FROM stg_order_items i
LEFT JOIN stg_order_reviews r
    ON i.order_id = r.order_id
WHERE (i.order_id, i.product_id, i.seller_id) NOT IN (
    SELECT order_id, product_id, seller_id FROM dim_items
);

-- Payments Dimension Table Incremental Load

INSERT INTO dim_payments (
    order_id,
    payment_type,
    payment_installments,
    payment_value
)
SELECT
    p.order_id,
    p.payment_type,
    p.payment_installments,
    CAST(p.payment_value AS FLOAT4)
FROM stg_order_payments p
WHERE p.order_id NOT IN (SELECT order_id FROM dim_payments);

-- Orders Fact Table Incremental Load: One Row Per Order

-- Insert one row per unique order_id, using safe aggregates

INSERT INTO fact_orders (
    order_id,
    customer_id,
    order_purchase_timestamp,
    customer_city,
    product_id,
    seller_id,
    price,
    freight_value,
    seller_city,
    product_category_name_english,
    payment_type,
    payment_installments,
    payment_value
)
SELECT
    o.order_id,
    o.customer_id,
    TO_TIMESTAMP(o.order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS order_purchase_timestamp,
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
FROM (
    SELECT DISTINCT order_id, customer_id, order_purchase_timestamp
    FROM stg_orders
) o
JOIN dim_customers c ON o.customer_id = c.customer_id

LEFT JOIN (
    SELECT order_id,
           MIN(product_id) AS product_id,
           MIN(seller_id) AS seller_id,
           MIN(price) AS price,
           MIN(freight_value) AS freight_value
    FROM dim_items
    GROUP BY order_id
) i ON o.order_id = i.order_id

LEFT JOIN dim_sellers s ON i.seller_id = s.seller_id
LEFT JOIN dim_products p ON i.product_id = p.product_id

LEFT JOIN (
    SELECT order_id,
           MIN(payment_type) AS payment_type,
           MAX(payment_installments) AS payment_installments,
           SUM(payment_value) AS payment_value
    FROM dim_payments
    GROUP BY order_id
) pay ON o.order_id = pay.order_id;

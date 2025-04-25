import psycopg2
import pandas as pd
import os

conn = psycopg2.connect(
    host=os.getenv('REDSHIFT_HOST'),
    dbname=os.getenv('REDSHIFT_DB'),
    user=os.getenv('REDSHIFT_USER'),
    password=os.getenv('REDSHIFT_PASSWORD'),
    port=os.getenv('REDSHIFT_PORT')
)

query = """
SELECT 
    o.customer_id,
    c.customer_city,
    c.customer_state,
    c.geolocation_lat,
    c.geolocation_lng,
    oi.order_id,
    oi.price + oi.freight_value AS total_paid,
    oi.price AS product_price,
    oi.freight_value AS shipping_cost,
    oi.review_score
FROM fact_order_items oi
JOIN fact_orders o ON oi.order_id = o.order_id
JOIN dim_customers c ON o.customer_id = c.customer_id
ORDER BY total_paid DESC;
"""

df = pd.read_sql(query, conn)
df.to_csv("BI/single_customer_view.csv", index=False)
conn.close()

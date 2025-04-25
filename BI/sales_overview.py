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
    c.customer_state,
    c.customer_city,
    DATE_TRUNC('month', f.order_purchase_timestamp) AS order_month,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(fo.price + fo.freight_value) AS total_sales,
    AVG(fo.price + fo.freight_value) AS avg_order_value
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
JOIN fact_order_items fo ON f.order_id = fo.order_id
GROUP BY c.customer_state, c.customer_city, DATE_TRUNC('month', f.order_purchase_timestamp)
ORDER BY order_month, c.customer_state;
"""

df = pd.read_sql(query, conn)
df.to_csv("BI/sales_overview.csv", index=False)
conn.close()

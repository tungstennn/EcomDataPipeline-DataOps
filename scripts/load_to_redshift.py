from extract_from_s3 import extract_all_data
from transform_data import (
    transform_dim_customers,
    transform_dim_sellers,
    transform_dim_products,
    transform_dim_items,
    transform_dim_payments,
    transform_orders,
    transform_fact_orders
)
import boto3
from io import BytesIO

s3 = boto3.client("s3")
BUCKET_NAME = "ecom-sales-bucket"
S3_PREFIX = "transformed_data/"

def upload_df_to_s3(df, filename):
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{S3_PREFIX}{filename}", Body=buffer.getvalue())
    print(f"✅ Uploaded {filename} to S3")

if __name__ == "__main__":
    data = extract_all_data()

    dim_customers = transform_dim_customers(data['olist_customers_dataset'], data['olist_geolocation_dataset'])
    dim_sellers = transform_dim_sellers(data['olist_sellers_dataset'], data['olist_geolocation_dataset'])
    dim_products = transform_dim_products(data['olist_products_dataset'], data['product_category_name_translation'])
    dim_items = transform_dim_items(data['olist_order_items_dataset'], data['olist_order_reviews_dataset'])
    dim_payments = transform_dim_payments(data['olist_order_payments_dataset'])
    orders_df = transform_orders(data['olist_orders_dataset'])
    
    fact_orders = transform_fact_orders(
        orders_df,
        dim_customers,
        dim_sellers,
        dim_products,
        dim_items,
        dim_payments
    )

    # Upload all to S3
    upload_df_to_s3(fact_orders, "fact_orders.csv")
    upload_df_to_s3(dim_customers, "dim_customers.csv")
    upload_df_to_s3(dim_sellers, "dim_sellers.csv")
    upload_df_to_s3(dim_products, "dim_products.csv")
    upload_df_to_s3(dim_items, "dim_items.csv")
    upload_df_to_s3(dim_payments, "dim_payments.csv")
    upload_df_to_s3(orders_df, "dim_orders.csv")

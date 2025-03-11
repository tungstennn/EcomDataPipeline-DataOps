import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
from io import BytesIO

# 🔹 AWS S3 Configuration
S3_BUCKET = "ecom-sales-bucket"
S3_FOLDER = "olist_data/"  # Folder where raw data is stored
S3_OUTPUT_FOLDER = "processed_data/"  # Where transformed data will be saved

# 🔹 Initialize S3 Client
s3 = boto3.client("s3")

def read_s3_csv(file_name):
    """Reads a CSV file from S3 into a Pandas DataFrame"""
    s3_key = f"{S3_FOLDER}{file_name}"
    obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    return pd.read_csv(obj["Body"])

def transform_data():
    """Extracts data from S3, applies transformations, and stores as Parquet"""
    
    print("🚀 Reading raw data from S3...")
    orders_df = read_s3_csv("olist_orders_dataset.csv")
    customers_df = read_s3_csv("olist_customers_dataset.csv")
    products_df = read_s3_csv("olist_products_dataset.csv")

    print("📊 Applying Transformations...")
    
    # 🔹 Fact Table: Orders (Transactional Data)
    fact_orders = orders_df[["order_id", "customer_id", "order_status", "order_purchase_timestamp"]]
    
    # 🔹 Dimension Tables
    dim_customers = customers_df[["customer_id", "customer_unique_id", "customer_city", "customer_state"]]
    dim_products = products_df[["product_id", "product_category_name"]]

    # Convert DataFrames to Parquet
    def save_parquet(df, file_name):
        table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer)
        s3.put_object(Bucket=S3_BUCKET, Key=f"{S3_OUTPUT_FOLDER}{file_name}", Body=buffer.getvalue())
        print(f"✅ {file_name} saved in S3!")

    print("📡 Uploading Transformed Data to S3...")
    save_parquet(fact_orders, "fact_orders.parquet")
    save_parquet(dim_customers, "dim_customers.parquet")
    save_parquet(dim_products, "dim_products.parquet")

if __name__ == "__main__":
    transform_data()

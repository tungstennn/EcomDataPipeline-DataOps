import boto3
import psycopg2
import os

# 🔹 AWS Redshift Configuration
REDSHIFT_HOST = "your-redshift-cluster-endpoint"
REDSHIFT_DB = "dataops-warehouse"
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
S3_BUCKET = "ecom-sales-bucket"
S3_FOLDER = "processed_data/"

# 🔹 Connect to Redshift
conn = psycopg2.connect(
    dbname=REDSHIFT_DB,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD,
    host=REDSHIFT_HOST,
    port="5439"
)
cursor = conn.cursor()

def load_table(table_name, file_name):
    """Load transformed data from S3 into Redshift"""
    s3_path = f"s3://{S3_BUCKET}/{S3_FOLDER}{file_name}"
    
    sql = f"""
    COPY {table_name}
    FROM '{s3_path}'
    IAM_ROLE 'your-redshift-iam-role'
    FORMAT AS PARQUET;
    """
    
    cursor.execute(sql)
    conn.commit()
    print(f"✅ {table_name} loaded into Redshift!")

if __name__ == "__main__":
    print("🚀 Loading Transformed Data into Redshift...")
    load_table("fact_orders", "fact_orders.parquet")
    load_table("dim_customers", "dim_customers.parquet")
    load_table("dim_products", "dim_products.parquet")

    cursor.close()
    conn.close()

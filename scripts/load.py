import boto3
from io import BytesIO

# S3 setup
s3 = boto3.client("s3")
BUCKET_NAME = "ecom-sales-bucket"
S3_PREFIX = "transformed_data/"

def upload_df_to_s3(df, filename):
    """
    Upload a single DataFrame to S3 as a CSV file.
    """
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{S3_PREFIX}{filename}", Body=buffer.getvalue())
    print(f"✅ Uploaded {filename} to S3")


def load_to_s3(transformed_data):
    
    print("📤 Uploading transformed data to S3...\n")
    
    mapping = {
        "fact_orders": "fact_orders.csv",
        "dim_customers": "dim_customers.csv",
        "dim_sellers": "dim_sellers.csv",
        "dim_products": "dim_products.csv",
        "dim_items": "dim_items.csv",
        "dim_payments": "dim_payments.csv",
        "orders": "dim_orders.csv"
    }

    for key, filename in mapping.items():
        if key in transformed_data:
            upload_df_to_s3(transformed_data[key], filename)
    
    print("\n✅ All transformed data uploaded to S3.")

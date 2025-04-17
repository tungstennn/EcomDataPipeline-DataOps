import boto3 # AWS SDK for Python
import pandas as pd 
from io import BytesIO 

S3_BUCKET = 'ecom-sales-bucket' # S3 bucket name
S3_PREFIX = 'olist_data/'   # S3 folder name
S3_FILES = [                # List of CSV files to extract
    'olist_orders_dataset.csv',
    'olist_customers_dataset.csv',
    'olist_order_items_dataset.csv',
    'olist_order_payments_dataset.csv',
    'olist_order_reviews_dataset.csv',
    'olist_products_dataset.csv',
    'olist_sellers_dataset.csv',
    'product_category_name_translation.csv',
    'olist_geolocation_dataset.csv'
]



# This function retrieves a CSV file from S3 and returns it as a pandas DataFrame
def get_dataframe_from_s3(file_name):
    s3 = boto3.client('s3')
    key = f"{S3_PREFIX}{file_name}"
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = response['Body'].read()
    return pd.read_csv(BytesIO(content))



# This function extracts all CSV files from S3 and returns them as a dictionary of DataFrames
def extract_all_data():
    data = {}
    for file in S3_FILES:
        print(f"Extracting {file}...")
        df = get_dataframe_from_s3(file)
        name = file.replace(".csv", "")
        data[name] = df

    return data
if __name__ == "__main__":
    all_data = extract_all_data()

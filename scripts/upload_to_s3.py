import boto3 # AWS SDK for Python
import os

S3_BUCKET = "ecom-sales-bucket" # S3 bucket name
S3_FOLDER = "olist_data/" # S3 folder name
LOCAL_FOLDER = "raw_data/" # Local folder containing CSV files

s3 = boto3.client("s3") # Initialize S3 client

# This function uploads all CSV files in the specified local folder to the specified S3 bucket and folder.
def upload_csv_files():
    """Uploads CSVs in raw_data/ to S3."""
    for file_name in os.listdir(LOCAL_FOLDER):
        if file_name.endswith(".csv"): # Check if the file is a CSV
            file_path = os.path.join(LOCAL_FOLDER, file_name)
            s3_key = f"{S3_FOLDER}{file_name}"

            print(f"📤 Uploading {file_name} to S3...")
            s3.upload_file(file_path, S3_BUCKET, s3_key)
            print(f"✅ {file_name} uploaded successfully!")

if __name__ == "__main__":
    upload_csv_files() 

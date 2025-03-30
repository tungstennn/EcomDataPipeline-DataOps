import boto3
import os

# Config
S3_BUCKET = "ecom-sales-bucket"
S3_FOLDER = "olist_data/"
LOCAL_FOLDER = "raw_data/"

s3 = boto3.client("s3")

def upload_csv_files():
    """Uploads CSVs in raw_data/ to S3."""
    for file_name in os.listdir(LOCAL_FOLDER):
        if file_name.endswith(".csv"):
            file_path = os.path.join(LOCAL_FOLDER, file_name)
            s3_key = f"{S3_FOLDER}{file_name}"

            print(f"📤 Uploading {file_name} to S3...")
            s3.upload_file(file_path, S3_BUCKET, s3_key)
            print(f"✅ {file_name} uploaded successfully!")

if __name__ == "__main__":
    upload_csv_files()

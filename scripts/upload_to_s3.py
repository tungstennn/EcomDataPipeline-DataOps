import boto3
import os

# 🔹 AWS S3 Configuration
S3_BUCKET = "raw-sales-bucket"
S3_FOLDER = "olist_data/"  # Folder in S3 where files will be stored
LOCAL_FOLDER = "raw_data/"  # Folder where the dataset is stored locally

# 🔹 Initialize S3 Client
s3 = boto3.client("s3")

def upload_files():
    """Uploads all files in raw_data/ to S3"""
    for file_name in os.listdir(LOCAL_FOLDER):
        file_path = os.path.join(LOCAL_FOLDER, file_name)
        s3_key = f"{S3_FOLDER}{file_name}"  # Define S3 object key

        print(f"📡 Uploading {file_name} to S3...")
        s3.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"✅ {file_name} uploaded successfully!")

if __name__ == "__main__":
    upload_files()

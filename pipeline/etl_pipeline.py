from scripts.extract import extract_all_data
from scripts.transform import transform
from scripts.load import load_to_s3
#from etl.copy import copy_to_redshift


def run_pipeline():
    print("Starting ETL pipeline...")
    
    # Extract data from S3
    print('Extract')
    raw_data = extract_all_data()
    print("Data extracted successfully!")
    
    # Transform data locally
    transformed_data = transform(raw_data)
    print("Data transformed successfully!")
    
    # Load transformed data to S3
    load_to_s3(transformed_data)
    print("Data loaded to S3 successfully!")
    print('---------------------------------------')
    print("ETL pipeline completed successfully!")
    
if __name__ == "__main__":
    run_pipeline()
from scripts.extract import extract_all_data
from scripts.transform import transform
from scripts.load import load_to_s3
from scripts.create_redshift_tables import create_tables
from dotenv import load_dotenv


def run_pipeline():
    print("Starting ETL pipeline...")
    print('---------------------------------------')
    
    # Extract data from S3
    print('\nExtracting data from S3...')
    raw_data = extract_all_data()
    print("\n✅ Data extracted successfully!")
    
    # Transform data locally
    print('\nTransforming data...')
    transformed_data = transform(raw_data)
    print("\n✅ Data transformed successfully!")
    
    # Load transformed data to S3
    print('\nLoading into S3...')
    load_to_s3(transformed_data)
    print("\n✅ Data loaded to S3 successfully!")
    print('---------------------------------------')
    print("\n✅ ETL pipeline completed successfully!")
    
    # Create tables in Redshift
    print('\nCreating tables in Redshift...')
    create_tables()
    print("\n✅ Tables created successfully!")
    
if __name__ == "__main__":
    run_pipeline()
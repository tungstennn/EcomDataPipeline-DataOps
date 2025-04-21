#from scripts.extract import extract_all_data
#from scripts.transform import transform
#from scripts.load import load_to_s3
#from dotenv import load_dotenv

import sys
import os

sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))

from scripts.create_redshift_tables import create_tables
from scripts.redshift_utils import connect_to_redshift, close_redshift_connection
from scripts.copy_to_redshift import copy_data
from scripts.sql_transform import transform


def run_pipeline():
    conn, cur = connect_to_redshift()
    
    if conn is None or cur is None:
        print("❌ Failed to create Redshift connection. Exiting.")
        return
    try:
        create_tables(conn, cur)
        copy_data(conn,cur)
        transform(conn,cur)
        conn.commit()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
        
    finally:
        close_redshift_connection(conn, cur)
    
    
if __name__ == "__main__":
    run_pipeline()
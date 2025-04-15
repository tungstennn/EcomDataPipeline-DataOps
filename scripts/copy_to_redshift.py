from dotenv import load_dotenv
import os
import psycopg2

# Load .env from the parent directory (project root)
load_dotenv(dotenv_path='.env')

def copy_data(conn, cur):
    try:
        print("📦 Copying data to Redshift...")
        with open('sql/copy_to_redshift.sql', 'r') as f:
            sql_script = f.read()
        cur.execute(sql_script)
        print("✅ Data copied successfully")
    except Exception as e:
        print(f"❌ Error copying data: {e}")
        conn.rollback()
from dotenv import load_dotenv
import os
import psycopg2

# Load .env from the parent directory (project root)
load_dotenv(dotenv_path='.env')

def transform(conn, cur):
    # Transform tables in Redshift using the SQL script.
    try:
        print("📜 Transforming...")
        with open('sql/incremental_transform.sql', 'r') as f:
            sql_script = f.read()

        cur.execute(sql_script)
        conn.commit()

        print("✅ Tables transformed successfully")
        
    except Exception as e:
        print(f"❌ Error transforming tables: {e}")
        conn.rollback()



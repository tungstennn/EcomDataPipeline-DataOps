from dotenv import load_dotenv
import os
import psycopg2

# Load .env from the parent directory (project root)
load_dotenv(dotenv_path='.env')

def create_tables(conn, cur):
    # Create tables in Redshift using the SQL script.
    try:
        print("📜 Creating tables in Redshift...")
        with open('sql/create_tables.sql', 'r') as f:
            sql_script = f.read()

        cur.execute(sql_script)
        conn.commit()

        print("✅ Tables created successfully")
        
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        conn.rollback()



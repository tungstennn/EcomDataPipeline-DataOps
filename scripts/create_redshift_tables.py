from dotenv import load_dotenv
import os
import psycopg2

# Load .env from the parent directory (project root)
load_dotenv(dotenv_path='.env')

def create_tables():
    # Create tables in Redshift using the SQL script.

    conn = psycopg2.connect(
        host=os.getenv('REDSHIFT_HOST'),
        port=os.getenv('REDSHIFT_PORT'),
        user=os.getenv('REDSHIFT_USER'),
        password=os.getenv('REDSHIFT_PASSWORD'),
        dbname=os.getenv('REDSHIFT_DB')
    )
    print("🔗 Connected to Redshift")
    
    cur = conn.cursor()
    print("📜 Creating tables in Redshift...")
    with open('sql/create_tables.sql', 'r') as f:
        sql_script = f.read()

    cur.execute(sql_script)
    conn.commit()

    cur.close()
    conn.close()
    #print("✅ Tables created successfully.")
    
create_tables()
#print("Host:", os.getenv("REDSHIFT_HOST"))


import psycopg2
import os

def connect_to_redshift():
    try:
        conn = psycopg2.connect(
            host=os.getenv('REDSHIFT_HOST'),
            port=os.getenv('REDSHIFT_PORT'),
            user=os.getenv('REDSHIFT_USER'),
            password=os.getenv('REDSHIFT_PASSWORD'),
            dbname=os.getenv('REDSHIFT_DB')
        )
        print("🔗 Connected to Redshift")
        return conn, conn.cursor()
    except Exception as e:
        print(f"❌ Error connecting to Redshift: {e}")
        return None, None

def close_redshift_connection(conn, cur):
    if cur: cur.close()
    if conn: conn.close()
    print("🔒 Redshift connection closed")

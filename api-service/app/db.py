import psycopg2
from psycopg2.extras import RealDictCursor
from .config import Config

def get_db_connection():
    
    print(f"DEBUG: Connecting to {Config.DB_NAME} on {Config.DB_HOST}:{Config.DB_PORT} as {Config.DB_USER}")
    
    try:
        conn = psycopg2.connect(
            dbname=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            cursor_factory=RealDictCursor,
            connect_timeout=3
        )
        return conn
    except Exception as e:
        print(f"❌ DATABASE CONNECTION FAILED: {e}")
        return None
import os
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pydantic import ValidationError
from models import BusinessContract

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

def transform_business_v2():
    print("Starting Business Transformation...")
    total_processed = 0
    total_quarantined = 0
    batch_data = []
    
    conn = None
    try:
        conn = get_db_connection()
        conn.autocommit = False
        
        
        read_cur = conn.cursor(name='business_stream')
        read_cur.itersize = 5000
        write_cur = conn.cursor()

        read_cur.execute("SELECT data, row_hash FROM raw_business")

        for data, r_hash in read_cur:
            try:
                
                v = BusinessContract(**data)
                
                batch_data.append((
                    v.business_id, v.name, v.address, v.city, v.state, 
                    v.postal_code, v.latitude, v.longitude, v.stars, 
                    v.review_count, v.is_open, v.categories, r_hash
                ))
                
                if len(batch_data) >= 5000:
                    query = """
                        INSERT INTO dim_business (
                            business_id, name, address, city, state, 
                            postal_code, latitude, longitude, stars, 
                            review_count, is_open, categories, row_hash
                        ) VALUES %s ON CONFLICT (business_id) DO NOTHING;
                    """
                    execute_values(write_cur, query, batch_data)
                    total_processed += len(batch_data)
                    print(f"Businesses: {total_processed}")
                    batch_data = []

            except ValidationError as e:
                total_quarantined += 1
                write_cur.execute(
                    "INSERT INTO bad_records (source_table, raw_data, error_message) VALUES (%s, %s, %s)",
                    ('dim_business', json.dumps(data), str(e))
                )

        if batch_data:
            execute_values(write_cur, query, batch_data)
            total_processed += len(batch_data)

        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()
        print(f"Processed: {total_processed} | Quarantined: {total_quarantined}")

if __name__ == "__main__":
    transform_business_v2()
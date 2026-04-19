import os
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pydantic import ValidationError
from models import UserContract

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

def transform_user_v2():
    print("Starting User Transformation...")
    total_processed = 0
    total_quarantined = 0
    batch_data = []
    
    conn = None
    try:
        conn = get_db_connection()
        conn.autocommit = False
        
        read_cur = conn.cursor(name='user_stream')
        read_cur.itersize = 10000
        write_cur = conn.cursor()

        read_cur.execute("SELECT data, row_hash FROM raw_user")

        for data, r_hash in read_cur:
            try:
                v = UserContract(**data)
                
                batch_data.append((
                    v.user_id, v.name, v.review_count, v.yelping_since,
                    v.useful, v.funny, v.cool, v.fans, v.average_stars, r_hash
                ))
                
                if len(batch_data) >= 10000:
                    query = """
                        INSERT INTO dim_user (
                            user_id, name, review_count, yelping_since,
                            useful, funny, cool, fans, average_stars, row_hash
                        ) VALUES %s ON CONFLICT (user_id) DO NOTHING;
                    """
                    execute_values(write_cur, query, batch_data)
                    total_processed += len(batch_data)
                    print(f"Users: {total_processed}")
                    batch_data = []

            except ValidationError as e:
                total_quarantined += 1
                write_cur.execute(
                    "INSERT INTO bad_records (source_table, raw_data, error_message) VALUES (%s, %s, %s)",
                    ('dim_user', json.dumps(data), str(e))
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
        print(f"DONE. Processed: {total_processed} | Quarantined: {total_quarantined}")

if __name__ == "__main__":
    transform_user_v2()
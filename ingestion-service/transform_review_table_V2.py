import os
import logging
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pydantic import ValidationError
from models import ReviewContract 


logging.basicConfig(
    filename='review_transformation_v2.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

def transform_reviews_v2():
    
    total_processed = 0
    total_quarantined = 0
    batch_data = []
    batch_size = 10000 
    
    conn = None
    try:
        conn = get_db_connection()
        
        conn.autocommit = False 
        
        
        read_cur = conn.cursor(name='v2_final_stream') 
        read_cur.itersize = 10000 
        
        write_cur = conn.cursor()

        
        read_cur.execute("SELECT data, row_hash FROM raw_review")

        for data, r_hash in read_cur:
            try:
                validated = ReviewContract(**data)
                batch_data.append((
                    validated.review_id, validated.user_id, validated.business_id,
                    validated.stars, validated.useful, validated.funny, validated.cool,
                    validated.text, validated.date, r_hash
                ))
                
                if len(batch_data) >= batch_size:
                    insert_query = """
                        INSERT INTO fact_review (
                            review_id, user_id, business_id, stars, 
                            useful, funny, cool, text, date, row_hash
                        ) VALUES %s 
                        ON CONFLICT (review_id, date) DO NOTHING;
                    """
                    execute_values(write_cur, insert_query, batch_data)
                    
                    total_processed += len(batch_data)
                    print(f"Processed: {total_processed} | Quarantined: {total_quarantined}")
                    batch_data = [] 

            except ValidationError as e:
                total_quarantined += 1
                write_cur.execute(
                    "INSERT INTO bad_records (source_table, raw_data, error_message) VALUES (%s, %s, %s)",
                    ('fact_review', json.dumps(data), str(e))
                )

        # Final flush and ONE big commit at the end
        if batch_data:
            execute_values(write_cur, insert_query, batch_data)
            total_processed += len(batch_data)
        
        conn.commit()
        print("Finalizing transaction...")

    except Exception as e:
        print(f"Critical failure: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
        

if __name__ == "__main__":
    transform_reviews_v2()
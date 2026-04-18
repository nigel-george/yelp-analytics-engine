import os
import logging
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(
    filename='review_transformation_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()

def get_db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    conn.autocommit = True 
    return conn

def setup_silver_review_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fact_review (
        review_id VARCHAR(22) PRIMARY KEY,
        user_id VARCHAR(22),
        business_id VARCHAR(22),
        stars FLOAT,
        useful INT,
        funny INT,
        cool INT,
        text TEXT,
        date TIMESTAMP,
        row_hash VARCHAR(64),
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

def transform_reviews():
    
    total_processed = 0
    conn = None
    
    try:
        conn = get_db_connection()
        
        read_cur = conn.cursor() 
        read_cur.itersize = 10000 
        
        write_cur = conn.cursor()

        
        setup_silver_review_table(write_cur)
        read_cur.execute("SELECT data, row_hash FROM raw_review")

        batch_data = []
        batch_size = 25000

        for data, r_hash in read_cur:
            try:
                batch_data.append((
                    data.get('review_id'),
                    data.get('user_id'),
                    data.get('business_id'),
                    float(data.get('stars', 0.0)),
                    int(data.get('useful', 0)),
                    int(data.get('funny', 0)),
                    int(data.get('cool', 0)),
                    data.get('text'),
                    data.get('date'),
                    r_hash
                ))
                
                if len(batch_data) >= batch_size:
                    insert_query = """
                        INSERT INTO fact_review (
                            review_id, user_id, business_id, stars, 
                            useful, funny, cool, text, date, row_hash
                        ) VALUES %s 
                        ON CONFLICT (review_id) DO NOTHING;
                    """
                    execute_values(write_cur, insert_query, batch_data)
                    total_processed += len(batch_data)
                    print(f"Total reviews processed: {total_processed}...")
                    batch_data = [] 

            except Exception as e:
                logging.error(f"Error parsing a record: {e}")
                continue

        
        if batch_data:
            execute_values(write_cur, insert_query, batch_data)
            total_processed += len(batch_data)

    except Exception as e:
        print(f"Critical failure: {e}")
        logging.critical(f"Script crashed: {e}")
    finally:
        if conn:
            conn.close()
        print(f"Final Step: Successfully structured {total_processed} reviews.")

if __name__ == "__main__":
    transform_reviews()
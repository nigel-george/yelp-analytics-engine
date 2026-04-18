import os
import logging
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# 1. Setup Logging
logging.basicConfig(
    filename='business_transformation_errors.log',
    level=logging.ERROR,
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

def setup_silver_layer(cursor):
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dim_business (
        business_id VARCHAR(22) PRIMARY KEY,
        name TEXT NOT NULL,
        address TEXT,
        city TEXT,
        state VARCHAR(5),
        postal_code VARCHAR(10),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        stars FLOAT,
        review_count INT,
        is_open INT,
        categories TEXT,
        hours JSONB,
        attributes JSONB,
        row_hash VARCHAR(64),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_query)

def transform_businesses():
    conn = get_db_connection()
    
    read_cur = conn.cursor(name='business_reader', withhold=True)
    write_cur = conn.cursor()

    try:
        setup_silver_layer(write_cur)
        conn.commit()

        batch_size = 5000
        read_cur.execute("SELECT data, row_hash FROM raw_business")

        while True:
            records = read_cur.fetchmany(batch_size)
            if not records:
                break

            processed_batch = []
            for data,r_hash in records:
                try:
                    # Map JSON keys to Table Columns
                    processed_batch.append((
                        data.get('business_id'),
                        data.get('name'),
                        data.get('address'),
                        data.get('city'),
                        data.get('state'),
                        data.get('postal_code'),
                        float(data.get('latitude')) if data.get('latitude') else None,
                        float(data.get('longitude')) if data.get('longitude') else None,
                        float(data.get('stars')) if data.get('stars') else 0.0,
                        int(data.get('review_count')) if data.get('review_count') else 0,
                        int(data.get('is_open')) if data.get('is_open') is not None else 0,
                        data.get('categories'),
                        psycopg2.extras.Json(data.get('hours')),
                        psycopg2.extras.Json(data.get('attributes')),
                        r_hash
                    ))
                except Exception as e:
                    logging.error(f"Failed to parse record {data.get('business_id', 'UNKNOWN')}: {e}")
                    continue

            # Upsert logic for Idempotency
            insert_query = """
                INSERT INTO dim_business (
                    business_id, name, address, city, state, postal_code,
                    latitude, longitude, stars, review_count, is_open, 
                    categories, hours, attributes, row_hash
                ) VALUES %s 
                ON CONFLICT (business_id) DO NOTHING;
            """
            
            execute_values(write_cur, insert_query, processed_batch)
            conn.commit()
            print(f"Ingested a batch of {len(processed_batch)} businesses...")

    except Exception as e:
        print(f"Critical script failure: {e}")
        logging.critical(f"Critical failure: {e}")
    finally:
        read_cur.close()
        write_cur.close()
        conn.close()

if __name__ == "__main__":
    transform_businesses()
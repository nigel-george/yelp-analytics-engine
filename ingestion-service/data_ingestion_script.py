import os
import hashlib
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

# Explicit configuration to avoid any "ghost" connections
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"), # Using IP instead of localhost for stability
    "port": os.getenv("DB_PORT")
}

def generate_hash(data_str):
    """Generates a unique MD5 hash for each line to ensure idempotency."""
    return hashlib.md5(data_str.encode('utf-8')).hexdigest()

def load_yelp_data(file_path, table_name, batch_size=10000):
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT current_database();")
        
        
        batch = []
        count = 0

        print(f"Starting ingestion for table: {table_name}")

        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                row_hash = generate_hash(line)
                batch.append((line, row_hash))
                
                if len(batch) >= batch_size:
                    
                    execute_values(
                        cur, 
                        f"INSERT INTO public.{table_name} (data, row_hash) VALUES %s ON CONFLICT (row_hash) DO NOTHING", 
                        batch
                    )
                    conn.commit()
                    count += len(batch)
                    print(f"Ingested {count} rows into {table_name}...")
                    batch = []

        if batch:
            execute_values(
                cur, 
                f"INSERT INTO public.{table_name} (data, row_hash) VALUES %s ON CONFLICT (row_hash) DO NOTHING", 
                batch
            )
            conn.commit()
            count += len(batch)

        cur.close()
        conn.close()
        print(f"Finished loading {table_name}. Total rows: {count}")

    except Exception as e:
        print(f"Error during ingestion for {table_name}: {e}")

if __name__ == "__main__":
    
    base_path = os.getenv("DATASET_BASE_PATH")

    if not base_path:
        print("Error: DATASET_BASE_PATH not found in .env file")
    else:
        
        business_json = os.path.join(base_path, 'yelp_academic_dataset_business.json')
        review_json = os.path.join(base_path, 'yelp_academic_dataset_review.json')
        user_json = os.path.join(base_path, 'yelp_academic_dataset_user.json')

        
        if os.path.exists(business_json):
            print(f"Dataset found at {base_path}. Starting ingestion...")
            load_yelp_data(business_json, 'raw_business')
            load_yelp_data(review_json, 'raw_review')
            load_yelp_data(user_json, 'raw_user')
        else:
            print(f"Critical Error: Files not found at {business_json}")
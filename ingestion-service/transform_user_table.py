import os
import logging
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# 1. Setup Logging for errors
logging.basicConfig(
    filename='user_transformation_errors.log',
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

def setup_silver_user_table(cursor):
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dim_user (
        user_id VARCHAR(22) PRIMARY KEY,
        name TEXT,
        review_count INT,
        yelping_since TIMESTAMP,
        average_stars FLOAT,
        fans INT,
        cool INT,
        funny INT,
        useful INT,
        friends TEXT,
        elite TEXT,
        compliment_hot INT,
        compliment_more INT,
        compliment_profile INT,
        compliment_cute INT,
        compliment_list INT,
        compliment_note INT,
        compliment_plain INT,
        compliment_cool INT,
        compliment_funny INT,
        compliment_writer INT,
        compliment_photos INT,
        row_hash VARCHAR(64),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_query)

def transform_users():
    conn = get_db_connection()
    read_cur = conn.cursor(name='user_transform_cursor', withhold=True) 
    write_cur = conn.cursor()

    try:
        
        setup_silver_user_table(write_cur)

        batch_size = 10000 
        read_cur.execute("SELECT data, row_hash FROM raw_user")

        while True:
            records = read_cur.fetchmany(batch_size)
            if not records:
                break

            processed_batch = []
            for data, r_hash in records:
                try:
                    processed_batch.append((
                        data.get('user_id'),
                        data.get('name'),
                        int(data.get('review_count', 0)),
                        data.get('yelping_since'),
                        float(data.get('average_stars', 0.0)),
                        int(data.get('fans', 0)),
                        int(data.get('cool', 0)),
                        int(data.get('funny', 0)),
                        int(data.get('useful', 0)),
                        data.get('friends'),
                        data.get('elite'),
                        int(data.get('compliment_hot', 0)),
                        int(data.get('compliment_more', 0)),
                        int(data.get('compliment_profile', 0)),
                        int(data.get('compliment_cute', 0)),
                        int(data.get('compliment_list', 0)),
                        int(data.get('compliment_note', 0)),
                        int(data.get('compliment_plain', 0)),
                        int(data.get('compliment_cool', 0)),
                        int(data.get('compliment_funny', 0)),
                        int(data.get('compliment_writer', 0)),
                        int(data.get('compliment_photos', 0)),
                        r_hash
                    ))
                except Exception as e:
                    logging.error(f"Parse error for user {data.get('user_id')}: {e}")
                    continue

            # Idempotent Insert
            insert_query = """
                INSERT INTO dim_user (
                    user_id, name, review_count, yelping_since, average_stars,
                    fans, cool, funny, useful, friends, elite,
                    compliment_hot, compliment_more, compliment_profile, compliment_cute,
                    compliment_list, compliment_note, compliment_plain, compliment_cool,
                    compliment_funny, compliment_writer, compliment_photos, row_hash
                ) VALUES %s 
                ON CONFLICT (user_id) DO NOTHING;
            """
            
            execute_values(write_cur, insert_query, processed_batch)
            print(f"Processed batch of {len(processed_batch)} users...")

    except Exception as e:
        print(f"Script failed: {e}")
        logging.critical(f"Critical script failure: {e}")
    finally:
        read_cur.close()
        write_cur.close()
        conn.close()
        print("User transformation complete.")

if __name__ == "__main__":
    transform_users()
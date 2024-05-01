import psycopg2
from psycopg2 import sql
from db_connection_pool import get_db_pool

db_pool = get_db_pool()

# Creates Sizes, Locations, and Posts(template) tables in the database
#   Only needs to be run once, or when things are reset, but this gives
#       a good picture of what the structure is like for these tables
def create_initial_tables():
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Sizes (
                file VARCHAR NOT NULL,
                block INT NOT NULL,
                size INT NOT NULL,
                cumulative_size BIGINT
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Locations (
                id VARCHAR PRIMARY KEY,
                file VARCHAR NOT NULL,
                block INT NOT NULL,
                row INT NOT NULL
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Posts (
                id VARCHAR NOT NULL,
                author VARCHAR,
                created_utc INT,
                subreddit VARCHAR,
                score INT,
                FOREIGN KEY (id) REFERENCES Locations(id)
            );
        """)

        conn.commit()
        print("Tables created successfully.")

    except Exception as e:
        print("An error occurred:", e)
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_initial_tables()

from psycopg2 import pool

# Needs to be modified for running on different system(s)
db_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=15,
    dbname="reddit",
    user="myuser",
    password="databas3",
    host="172.25.16.1"
)

def get_db_pool():
    return db_pool

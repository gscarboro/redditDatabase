from fastapi import FastAPI, Depends
from pydantic import BaseModel
from typing import Optional
import logging
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from retrievePost import get_post_by_id
from db_connection_pool import get_db_pool

# run the app in wsl: uvicorn main:app --reload
# documentation: http://127.0.0.1:8000/docs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
db_pool = get_db_pool()

ZST_BLOCKS_FILE_PATH = "data/"

# Pydantic model for the query, including default value(s)
class SearchQuery(BaseModel):
    author_exact: Optional[str] = None
    author_includes: Optional[str] = None
    author_excludes: Optional[str] = None
    subreddit_exact: Optional[str] = None
    subreddit_includes: Optional[str] = None
    subreddit_excludes: Optional[str] = None
    created_utc_exact: Optional[int] = None
    created_utc_min: Optional[int] = None
    created_utc_max: Optional[int] = None
    score_exact: Optional[int] = None
    score_min: Optional[int] = None
    score_max: Optional[int] = None
    id_exact: Optional[str] = None
    id_min: Optional[str] = None
    id_max: Optional[str] = None
    limit: Optional[int] = 100

# Select all tables that contain posts, which are tables with this naming convention: 
                                                # "{original_filename}_{YEARMMDD}_posts"
def get_post_tables(cursor):
    cursor.execute("SELECT tablename FROM pg_tables WHERE tablename LIKE '%_posts' AND schemaname = 'public'")
    return [row[0] for row in cursor.fetchall()]

# API Endpoint to search for posts by various criteria
@app.get("/search/posts")
async def search_posts(query: SearchQuery = Depends()):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        posts_tables = get_post_tables(cursor)
        combined_query = []
        params = []

        # Construct a query for each posts table so that all can be searched
        for table_name in posts_tables:
            individual_query = f"SELECT id FROM {table_name}"
            conditions = []

            if query.author_exact:
                conditions.append("author = %s")
                params.append(query.author_exact)
            if query.author_includes:
                conditions.append("author LIKE %s")
                params.append(f"%{query.author_includes}%")
            if query.author_excludes:
                conditions.append("author NOT LIKE %s")
                params.append(f"%{query.author_excludes}%")

            if query.subreddit_exact:
                conditions.append("subreddit = %s")
                params.append(query.subreddit_exact)
            if query.subreddit_includes:
                conditions.append("subreddit LIKE %s")
                params.append(f"%{query.subreddit_includes}%")
            if query.subreddit_excludes:
                conditions.append("subreddit NOT LIKE %s")
                params.append(f"%{query.subreddit_excludes}%")

            if query.created_utc_exact:
                conditions.append("created_utc = %s")
                params.append(query.created_utc_exact)
            if query.created_utc_min:
                conditions.append("created_utc >= %s")
                params.append(query.created_utc_min)
            if query.created_utc_max:
                conditions.append("created_utc <= %s")
                params.append(query.created_utc_max)

            if query.score_exact:
                conditions.append("score = %s")
                params.append(query.score_exact)
            if query.score_min:
                conditions.append("score >= %s")
                params.append(query.score_min)
            if query.score_max:
                conditions.append("score <= %s")
                params.append(query.score_max)

            if query.id_exact:
                conditions.append("id = %s")
                params.append(query.id_exact)
            if query.id_min:
                conditions.append("id >= %s")
                params.append(query.id_min)
            if query.id_max:
                conditions.append("id <= %s")
                params.append(query.id_max)

            if conditions:
                    individual_query += " WHERE " + " AND ".join(conditions)

            combined_query.append(individual_query)

        # Join all queries for different posts tables
        full_query = " UNION ALL ".join(combined_query)
        if query.limit:
            full_query += " LIMIT %s"
            params.append(query.limit)

        cursor.execute(full_query, tuple(params))

        posts = cursor.fetchall()
        full_posts = []

        # Use retrievePost functions to get each post
        for id in posts:
            full_posts.append(get_post_by_id(id))

        return full_posts
    
    finally:
        cursor.close()
        db_pool.putconn(conn)

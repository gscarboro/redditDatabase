import sys
import json
import struct
import psycopg2
from psycopg2 import pool
from typing import BinaryIO
from ZstBlocksFile import ZstBlocksFile, RowPosition
from db_connection_pool import get_db_pool

db_pool = get_db_pool()

# Gets the offset, or location to skip to, for a particular block in a file
#   This information is stored in the Sizes table
def get_block_offset(block_number, file_name):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT cumulative_size FROM sizes WHERE block = %s AND file = %s", (block_number, file_name))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        cursor.close()
        db_pool.putconn(conn)

# Uses location to get the actual full post from a previously created zst_blocks file
def get_post_at_position(file_path: str, block_number: int, post_index: int):
    with open(file_path, 'rb') as file:
        block_offset = get_block_offset(block_number, file_path)
        file.seek(block_offset)
        
        try:
            post_data = ZstBlocksFile.readBlockRowAt(file, RowPosition(block_offset, post_index))
            return json.loads(post_data.decode('utf-8'))
        except Exception as e:
            print(f"Error reading post at block {block_number}, post {post_index}: {str(e)}")

# Returns block number, file path, and row/index of a post
#   This information is stored in the Locations table
def get_post_details(post_id):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT block, file, row FROM locations WHERE id = %s", (post_id,))
        result = cursor.fetchone()
        return result if result else (None, None, None)
    finally:
        cursor.close()
        db_pool.putconn(conn)

# Calls the other relevant functions in order to get a particular post
def get_post_by_id(post_id):
    block_number, file_path, post_index = get_post_details(post_id)
    if not file_path:
        print(f"Post not found. block_number: {block_number}, path: {file_path}, post_index: {post_index}")
        return None

    print(f"Retrieving post {post_id} from file: {file_path}, block: {block_number}, row: {post_index}")
    return get_post_at_position(file_path, block_number, post_index)

if __name__ == "__main__":
    post_id = sys.argv[1] if len(sys.argv) > 1 else "0"

    post = get_post_by_id(post_id)
    if post:
        print("Retrieved post:")
        print(json.dumps(post, indent=4))
    else:
        print("Post could not be retrieved.")

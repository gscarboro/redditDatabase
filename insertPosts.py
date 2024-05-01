import sys
import os
import json
import psycopg2
import shutil
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime
from typing import Iterator
from fileStreams import getZstFileJsonStream
from ZstBlocksFile import ZstBlocksFile
from db_connection_pool import get_db_pool

db_pool = get_db_pool()

DEFAULT_MAX_POSTS = 10000
BLOCK_SIZE = 1000

def create_table(conn, posts_table_name):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {posts_table_name};")
    cursor.execute(f"""
        CREATE TABLE {posts_table_name} (
            id VARCHAR NOT NULL,
            author VARCHAR,
            created_utc INT,
            subreddit VARCHAR,
            score INT,
            FOREIGN KEY (id) REFERENCES Locations(id)
        );
    """)
    conn.commit()
    cursor.close()

def write_to_zst_blocks(posts: Iterator[tuple[int, dict]], output_path: str, block_size: int, max_blocks: int, posts_table_name: str, file_name: str):
    if os.path.exists(output_path):
        os.remove(output_path)

    with open(output_path, 'wb') as file, db_pool.getconn() as conn:
        cursor = conn.cursor()
        buffer = []
        blocks_written = 0
        post_index_in_block = 0
        cumulative_size = 0

        for _, post in posts:
            buffer.append(json.dumps(post).encode('utf-8'))
            cursor.execute("INSERT INTO locations (id, file, block, row) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
                        (post['id'], file_name, blocks_written, post_index_in_block))
            insert_query = f"INSERT INTO {posts_table_name} (id, author, created_utc, subreddit, score) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(insert_query, (post['id'], post['author'], post['created_utc'], post['subreddit'], post['score']))
            post_index_in_block += 1

            if len(buffer) == block_size:
                file_position_before = file.tell()
                ZstBlocksFile.writeBlocksStream(file, [buffer])
                file_position_after = file.tell()
                block_size_written = file_position_after - file_position_before

                cursor.execute("INSERT INTO sizes (file, block, size, cumulative_size) VALUES (%s, %s, %s, %s)", (file_name, blocks_written, block_size_written, cumulative_size))

                cumulative_size += block_size_written
                conn.commit()
                buffer.clear()
                blocks_written += 1
                post_index_in_block = 0
                if blocks_written >= max_blocks:
                    break

        if buffer and blocks_written < max_blocks:
            file_position_before = file.tell()
            ZstBlocksFile.writeBlocksStream(file, [buffer])
            file_position_after = file.tell()
            block_size_written = file_position_after - file_position_before
            cursor.execute("INSERT INTO sizes (file, block, size, cumulative_size) VALUES (%s, %s, %s, %s)", (file_name, blocks_written, block_size_written, cumulative_size))
            for buf_post in buffer:
                post_data = json.loads(buf_post.decode('utf-8'))
                cursor.execute("INSERT INTO locations (id, file, block, row) VALUES (%s, %s, %s, %s)", (post_data['id'], file_name, blocks_written, post_index_in_block))
            
            cumulative_size += block_size_written
            conn.commit()

        cursor.close()

def main():
    if len(sys.argv) < 2:
        print("Usage: insertPosts.py <directory_path> [max_posts]")
        return
    
    directory_path = sys.argv[1]
    max_posts = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_MAX_POSTS
    script_dir = os.path.dirname(os.path.realpath(__file__))
    zst_folder = os.path.join(script_dir, directory_path)
    processed_folder = os.path.join(script_dir, 'processed_zst')

    if not os.path.exists(processed_folder):
        os.makedirs(processed_folder)

    for filename in os.listdir(zst_folder):
        print(f"filename: {filename}")
        if filename.endswith('.zst'):
            input_file = os.path.join(zst_folder, filename)
            filename_without_extension = os.path.splitext(filename)[0]
            date_str = datetime.now().strftime("%Y%m%d")
            output_file = os.path.join(script_dir, 'data', f"{filename_without_extension}_{date_str}.zst_blocks")
            posts_table_name = f"{filename_without_extension}_{date_str}_posts"

            with db_pool.getconn() as conn:
                create_table(conn, posts_table_name)
            
            posts = getZstFileJsonStream(input_file)
            max_blocks = max_posts // BLOCK_SIZE if max_posts > BLOCK_SIZE else 1
            write_to_zst_blocks(posts, output_file, BLOCK_SIZE, max_blocks, posts_table_name, output_file)

            shutil.move(input_file, os.path.join(processed_folder, filename))
            print(f"Processed {filename}, {max_posts} posts")

if __name__ == "__main__":
    main()

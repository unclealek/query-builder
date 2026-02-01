import sqlite3
import requests
import json
from sql.models.base_query import BaseQuery
from sql.config import DB_NAME

def run_manual_sync(batch_size=5000):
    # 1. Get the current bookmark
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT last_processed_id FROM sync_metadata WHERE table_name = 'food_review'")
    last_id = cursor.fetchone()['last_processed_id']

    # 2. Use BaseQuery to get the next batch
    # We ask for all columns (*) but filter by our chunk_id
    query = BaseQuery(
        table="food_review",
        dimensions=["*"],
        condition=f"_chunk_id > {last_id}",
        order_by=["_chunk_id ASC"],
        limit=batch_size
    )

    sql = query.to_sql()
    cursor.execute(sql)
    rows = [dict(row) for row in cursor.fetchall()]

    if not rows:
        print("No new data to sync!")
        return

    # 3. Push to API (Placeholder for your Snowflake logic)
    print(f"Pushing rows with IDs > {last_id} (Batch Size: {len(rows)})...")

    # Example: response = requests.post(SNOWFLAKE_API_URL, json=rows, headers=HEADERS)
    # For now, let's assume success
    success = True

    if success:
        # 4. Update the Bookmark
        new_last_id = rows[-1]['_chunk_id']
        cursor.execute(
            "UPDATE sync_metadata SET last_processed_id = ?, last_sync_time = CURRENT_TIMESTAMP WHERE table_name = 'food_review'",
            (new_last_id,)
        )
        conn.commit()
        print(f"✅ Successfully synced up to ID: {new_last_id}")

    conn.close()

if __name__ == "__main__":
    run_manual_sync()

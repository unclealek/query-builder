import sqlite3
import json
import snowflake.connector
import tempfile
import os
from sql.models.base_query import BaseQuery
from sql.config import DB_NAME
from snowflaky.config import SF_CONFIG

def push_to_snowflake(batch_size=5000):
    conn_local = None
    ctx = None

    try:
        # 1. Connect to Local SQLite and get the bookmark
        conn_local = sqlite3.connect(DB_NAME)
        conn_local.row_factory = sqlite3.Row
        cursor_local = conn_local.cursor()

        # Initialize sync_metadata if it doesn't exist
        cursor_local.execute("""
            CREATE TABLE IF NOT EXISTS sync_metadata (
                table_name TEXT PRIMARY KEY,
                last_processed_id INTEGER DEFAULT 0,
                last_sync_time TIMESTAMP
            )
        """)

        cursor_local.execute("""
            INSERT OR IGNORE INTO sync_metadata (table_name, last_processed_id)
            VALUES ('food_review', 0)
        """)
        conn_local.commit()

        cursor_local.execute(
            "SELECT last_processed_id FROM sync_metadata WHERE table_name = ?",
            ('food_review',)
        )
        result = cursor_local.fetchone()
        last_id = result['last_processed_id'] if result else 0

        # 2. Fetch the next batch
        query = BaseQuery(
            table="food_review",
            dimensions=["*"],
            condition=f"_chunk_id > {last_id}",
            order_by=["_chunk_id ASC"],
            limit=batch_size
        )

        cursor_local.execute(query.to_sql())
        rows = [dict(row) for row in cursor_local.fetchall()]

        if not rows:
            print("✅ Everything is already in sync!")
            return

        # 3. Write data to temporary NDJSON file
        print(f"Preparing {len(rows)} rows for upload...")
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            tmp_filename = tmp_file.name
            for row in rows:
                tmp_file.write(json.dumps(row) + '\n')

        print(f"Temporary file created: {tmp_filename}")

        # 4. Connect to Snowflake
        print(f"Connecting to Snowflake...")
        ctx = snowflake.connector.connect(**SF_CONFIG)
        cs = ctx.cursor()

        try:
            # Start transaction
            cs.execute("BEGIN")

            # Create temporary stage if it doesn't exist
            cs.execute("""
                CREATE TEMPORARY STAGE IF NOT EXISTS temp_review_stage
                FILE_FORMAT = (TYPE = 'JSON')
            """)

            # Upload file to stage
            print("Uploading file to Snowflake stage...")
            cs.execute(f"PUT file://{tmp_filename} @temp_review_stage AUTO_COMPRESS=FALSE")

            # Copy data from stage to table
            print("Copying data into REVIEWS_STAGING...")
            cs.execute(f"""
                COPY INTO REVIEWS_STAGING (raw_data)
                FROM @temp_review_stage/{os.path.basename(tmp_filename)}
                FILE_FORMAT = (TYPE = 'JSON')
                MATCH_BY_COLUMN_NAME = NONE
            """)

            # Get the number of rows loaded
            result = cs.fetchone()
            rows_loaded = result[0] if result else 0
            print(f"   Loaded {rows_loaded} rows into Snowflake")

            # Commit Snowflake transaction
            cs.execute("COMMIT")

            # Update Local Bookmark
            new_last_id = rows[-1]['_chunk_id']
            cursor_local.execute(
                "UPDATE sync_metadata SET last_processed_id = ?, last_sync_time = CURRENT_TIMESTAMP WHERE table_name = ?",
                (new_last_id, 'food_review')
            )
            conn_local.commit()
            print(f"✅ Success! Snowflake is now synced up to ID: {new_last_id}")

        except Exception as e:
            print(f"❌ Error during Snowflake push: {e}")
            if cs:
                try:
                    cs.execute("ROLLBACK")
                    print("   Snowflake transaction rolled back")
                except:
                    pass
            raise

        finally:
            if cs:
                cs.close()
            # Clean up temporary file
            if os.path.exists(tmp_filename):
                os.unlink(tmp_filename)
                print(f"Temporary file cleaned up")

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if ctx:
            ctx.close()
        if conn_local:
            conn_local.close()

if __name__ == "__main__":
    push_to_snowflake()

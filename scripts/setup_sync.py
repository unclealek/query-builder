# scripts/setup_sync.py
import sqlite3
from sql.config import DB_NAME
from sql.registry.export_table import TABLE_REGISTRY

def setup_sync_infrastructure():
    table = TABLE_REGISTRY["food_review"]
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    print(f"🔧 Setting up sync infrastructure for {DB_NAME}...")

    # 1. Create the Metadata table (The Post-it Note)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_metadata (
            table_name TEXT PRIMARY KEY,
            last_processed_id INTEGER DEFAULT 0,
            last_sync_time DATETIME
        )
    """)

    # 2. Initialize the bookmark for your table if it's not there
    cursor.execute("""
        INSERT OR IGNORE INTO sync_metadata (table_name, last_processed_id)
        VALUES (?, 0)
    """, (table.name,))

    # 3. Verification: Ensure _chunk_id exists in the main table
    # (Since you already updated your legacy script, this is just a safety check)
    try:
        cursor.execute(f"SELECT _chunk_id FROM {table.name} LIMIT 1")
        print(f"✅ Verified: {table.name} has _chunk_id column.")
    except sqlite3.OperationalError:
        print(f"❌ Error: {table.name} is missing _chunk_id. Please re-run legacy migration.")

    conn.commit()
    conn.close()
    print("✅ Sync infrastructure is ready.")

if __name__ == "__main__":
    setup_sync_infrastructure()

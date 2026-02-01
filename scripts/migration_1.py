# sql/migration.py
import sqlite3
import json
import csv
from sql.config import DB_NAME, CSV_SOURCE
from sql.registry.export_table import TABLE_REGISTRY

def run_migration():
    table = TABLE_REGISTRY["food_review"]

    with open(table.schema_path, 'r') as f:
        schema = json.load(f)

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create table if not exists
    col_defs = []
    for col_name, col_info in schema['columns'].items():
        col_type = col_info['type']
        col_defs.append(f"{col_name} {col_type}")

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table.name} (
        _chunk_id INTEGER PRIMARY KEY AUTOINCREMENT,
        {', '.join(col_defs)}
        )
    """)

    # Load CSV in batches with deduplication
    batch_size = 50000
    batch = []

    with open(CSV_SOURCE, 'r') as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames

        for i, row in enumerate(reader):
            batch.append(tuple(row[col] for col in columns))

            if len(batch) >= batch_size:
                # Use INSERT OR IGNORE to skip duplicates
                placeholders = ', '.join(['?' for _ in columns])
                cursor.executemany(
                    f"""
                    INSERT OR IGNORE INTO {table.name}
                    ({', '.join(columns)})
                    VALUES ({placeholders})
                    """,
                    batch
                )
                batch = []
                print(f"Processed {i+1} rows...")

        # Insert remaining with deduplication
        if batch:
            placeholders = ', '.join(['?' for _ in columns])
            cursor.executemany(
                f"""
                INSERT OR IGNORE INTO {table.name}
                ({', '.join(columns)})
                VALUES ({placeholders})
                """,
                batch
            )

    conn.commit()
    cursor.execute(f"SELECT COUNT(*) FROM {table.name}")
    count = cursor.fetchone()[0]
    conn.close()

    print(f"✅ Loaded {count} unique rows into {table.name}")

if __name__ == "__main__":
    run_migration()

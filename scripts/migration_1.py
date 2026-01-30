# sql/migration.py
import sqlite3
import json
from sql.config import DB_NAME, CSV_SOURCE

def run_migration():
    # 1. Get table
    from sql.registry.export_table import TABLE_REGISTRY
    table = TABLE_REGISTRY["food_review"]

    # 2. Load schema
    with open(table.schema_path, 'r') as f:
        schema = json.load(f)

    # 3. Connect to SQLite
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # 4. Create table
    col_defs = []
    for col_name, col_info in schema['columns'].items():
        col_type = col_info['type']
        col_defs.append(f"{col_name} {col_type}")

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table.name} (
            {', '.join(col_defs)}
        )
    """)

    # 5. Load CSV
    import csv
    with open(CSV_SOURCE, 'r') as f:
        reader = csv.DictReader(f)

        # Insert data
        for row in reader:
            placeholders = ', '.join(['?' for _ in row])
            columns = ', '.join(row.keys())
            cursor.execute(
                f"INSERT INTO {table.name} ({columns}) VALUES ({placeholders})",
                list(row.values())
            )

    conn.commit()
    conn.close()

    print(f"Loaded {table.name} from {table.source}")

if __name__ == "__main__":
    run_migration()

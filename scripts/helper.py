import sqlite3

def create_test_table():
    """Generator: Creates the DB, yields tools, then closes connection."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        yield conn, cursor
    finally:
        conn.close()

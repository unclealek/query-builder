import sqlite3

def create_test_table():
    """Create empty in-memory table. No schema, no data."""
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    return conn, cursor

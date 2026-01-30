import sqlite3
import csv
from datetime import datetime


class BaseQuery:
    def __init__(
        self,
        table: str,
        dimensions: list[str],
        condition: str | None = None,
        group_by: list[str] | None = None,
        order_by:list[str] | None = None,
        alias: str | None = None,
    ):
        self.table = table
        self.dimensions = dimensions
        self.condition = condition
        self.group_by = group_by or []
        self.order_by = order_by or []
        self.alias = alias

    def to_sql(self) -> str:
        select_clause = ",\n  ".join(self.dimensions)

        sql = f"SELECT\n  {select_clause}\nFROM {self.table}"

        if self.condition:
            sql += f"\nWHERE {self.condition}"

        if self.group_by:
            sql += f"\nGROUP BY {', '.join(self.group_by)}"

        if self.order_by:
            sql += f"\nORDER BY {', '.join(self.order_by)}"

        if self.alias:
            sql = f"(\n{sql}\n) AS {self.alias}"

        return sql


    def execute_and_save(self, db_path: str = "food_reviews.db"):
        """Execute this query and auto-save to CSV."""
        # 1. Get SQL
        sql = self.to_sql()

        # 2. Execute
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        conn.close()

        # 3. Save to CSV
        db_stem = db_path.replace('.db', '')
        filename = f"{db_stem}.{self.table}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(column_names)
            writer.writerows(results)

        return results, filename

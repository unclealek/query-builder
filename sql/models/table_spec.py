import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Any
from .base_query import BaseQuery

class TableSpec:
    def __init__(
        self,
        name: str,
        schema_path: str,
        db_path: str,
        description: str = "",
    ):
        self.name = name
        self.schema_path = schema_path
        self.db_path = db_path # Path to your .db file
        self.description = description

        self._schema_data = None
        self._column_names = None

    def _load_schema(self) -> Dict[str, Any]:
        """Loads the JSON schema from disk."""
        if not Path(self.schema_path).exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")

        with open(self.schema_path, 'r') as f:
            self._schema_data = json.load(f)
        return self._schema_data

    @property
    def schema(self) -> Dict[str, Any]:
        if self._schema_data is None:
            self._load_schema()
        return self._schema_data

    @property
    def columns(self) -> List[str]:
        if self._column_names is None:
            self._column_names = list(self.schema.get('columns', {}).keys())
        return self._column_names

    def to_sql(self, dimensions: List[str] = None, condition: str = None) -> str:
        """Generates the SQL string using the BaseQuery builder."""
        query = BaseQuery(
            table=self.name,
            dimensions=dimensions or ["*"],
            condition=condition
        )
        return query.to_sql()

    def run_query(self, dimensions: List[str] = None, condition: str = None) -> List[Dict[str, Any]]:
        """Executes the generated SQL against the SQLite database."""
        sql = self.to_sql(dimensions, condition)

        # Connect to the SQLite file
        with sqlite3.connect(self.db_path) as conn:
            # This allows accessing columns by name: row['column_name']
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(sql)
            # Convert sqlite3.Row objects into standard dictionaries
            return [dict(row) for row in cursor.fetchall()]

    def __repr__(self):
        return f"TableSpec(name={self.name!r}, db={self.db_path!r})"

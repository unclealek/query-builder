import json
from pathlib import Path
from typing import Dict, Any
from .base_query import BaseQuery


class TableSpec:
    def __init__(
        self,
        name: str,
        schema_path: str,
        source: str,
        description: str = "",
        partition_by: list[str] = None,
        cluster_by: list[str] = None,
    ):
        self.name = name
        self.schema_path = schema_path
        self.source = source
        self.description = description
        self.partition_by = partition_by
        self.cluster_by = cluster_by

        self._schema_data = None
        self._column_names = None

    def _load_schema(self):
        if not Path(self.schema_path).exists():
            raise FileNotFoundError(f"Schema not found: {self.schema_path}")

        with open(self.schema_path, 'r') as f:
            data = json.load(f)

        if "columns" not in data:
            raise ValueError(f"Schema missing 'columns' key: {self.schema_path}")

        self._schema_data = data
        return self._schema_data

    @property
    def schema(self):
        if self._schema_data is None:
            self._load_schema()
        return self._schema_data

    @property
    def columns(self):
        if self._column_names is None:
            self._column_names = list(self.schema['columns'].keys())
        return self._column_names

    def query(self, dimensions=None, condition=None, **kwargs):
        return BaseQuery(
            table=self.name,
            dimensions=dimensions or ["*"],
            condition=condition,
            **kwargs,
        )
    def to_sql(self, dimensions=None, condition=None):
        query = self.query(dimensions, condition)
        return query.to_sql()
    
    def __repr__(self):
        return f"TableSpec(name={self.name!r}, source={self.source!r})"


    @classmethod
    def from_dict(cls, config):
        return cls(
            name=config['name'],
            schema_path=config['schema_path'],
        )
    def get_column_type(self, column_name):
        column_info = self.schema['columns'].get(column_name)
        return column_info.get('type', 'UNKNOWN') if column_info else 'UNKNOWN'

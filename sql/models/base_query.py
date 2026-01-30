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

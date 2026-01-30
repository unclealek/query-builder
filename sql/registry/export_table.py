from sql.models.table_spec import TableSpec

DB_FILE = "data/project.db"

TABLE_REGISTRY = {
    "food_review": TableSpec(
        name="food_review",
        schema_path="sql/cubes/food_review/schema.json",  #
        db_path=DB_FILE,
        description="Production food delivery app reviews",
    ),
}

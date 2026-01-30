from .models.table_creation.table_spec import TableSpec


TABLE_REGISTRY = {
    "food_reviews": TableSpec(
        name="food_reviews",
        schema_path="SQL/schemas/columns.json",  # Shared schema
        source="data/prod/food_reviews.csv",
        description="Production food delivery app reviews",
        partition_by=["date"],
        cluster_by=["app"]
    ),
}

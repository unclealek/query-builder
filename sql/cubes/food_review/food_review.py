from sql.models.base_query import BaseQuery
from sql.registry.export_table import TABLE_REGISTRY


def food_review_query():
    -> BaseQuery:
        table="food_review",
        dimensions=["*"],
    return BaseQuery(

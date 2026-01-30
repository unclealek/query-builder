from sql.models.base_query import BaseQuery

query = BaseQuery(
    table="quest_events",
    dimensions=[
        "organization_id",
        "user_id",
        "COUNTIF(result = 'failed') AS failed_count"
    ],
    condition="DATE(timestamp) = '2025-01-01'",
    group_by=["organization_id", "user_id"],
)

print(query.to_sql())

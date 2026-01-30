from sql.models.base_query import BaseQuery

query = BaseQuery(
    table="food_review",
    dimensions=["app_name", "score", "content"],
    condition="score >= 4.5"
)

results, filename = query.execute_and_save()
print(f"✅ Saved {len(results)} rows to {filename}")

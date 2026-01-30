from sql.models.base_query import BaseQuery
from sql.config import DB_NAME

query = BaseQuery(
    table="food_review",
    dimensions=["userName", "score", "content"],
    condition="score >= 4.5"
)

results, filename = query.execute_and_save(DB_NAME)
print(f"✅ Saved {len(results)} rows to {filename}")

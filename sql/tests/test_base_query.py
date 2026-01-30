import pytest
from ..models.base_query import BaseQuery


def test_base_query_to_sql():
    query = BaseQuery(
        table="test_table",
        dimensions=["col1", "col2", "COUNT(*) AS total"],
        condition="col1 > 100 AND col3 = 'active'",
        group_by=["col1", "col2"],
        order_by=["col1 DESC", "col2 ASC"],
        alias="test_summary",
    )

    expected_sql = """(
SELECT
  col1,
  col2,
  COUNT(*) AS total
FROM test_table
WHERE col1 > 100 AND col3 = 'active'
GROUP BY col1, col2
ORDER BY col1 DESC, col2 ASC
) AS test_summary"""

    result_sql = query.to_sql()
    assert result_sql == expected_sql

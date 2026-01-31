import pandas as pd
from scripts.helper import create_test_table
from ..food_review import food_review_query
from sql.tests.utils.utils import get_test_paths

def test_food_review_execution():
    paths = get_test_paths(__file__)

    # Get connection from generator
    db_gen = create_test_table()
    conn, cursor = next(db_gen)

    # 1. Setup Table & Mock Data
    df_mock = pd.read_csv(paths["mock"])
    df_mock.to_sql("food_review", conn, if_exists='replace', index=False)

    # 2. Run the query
    query_obj = food_review_query()
    actual_df = pd.read_sql_query(query_obj.to_sql(), conn)

    # 3. Load Ground Truth
    expected_df = pd.read_csv(paths["truth"])

    # 4. Clean Data
    actual_df = actual_df.fillna("")
    expected_df = expected_df.fillna("")

    # 5. Assert
    pd.testing.assert_frame_equal(actual_df, expected_df, check_like=True)

    # 6. Cleanup
    conn.close()

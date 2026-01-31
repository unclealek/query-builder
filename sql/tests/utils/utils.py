from pathlib import Path

def get_test_paths(test_file_path):
    """
    Given the __file__ path of a test, return paths for its assets.
    """
    test_dir = Path(test_file_path).parent
    return {
        "schema": test_dir / "schema.json",
        "mock": test_dir / "mock_data.csv",
        "truth": test_dir / "ground_truth.csv"
    }

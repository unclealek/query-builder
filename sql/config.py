from pathlib import Path

# This points to the root directory (where food_delivery_apps.csv is)
BASE_DIR = Path(__file__).resolve().parent.parent

# Standardized path for your database
DB_NAME = str(BASE_DIR / "sql" / "someday.db")

# Standardized path for your CSV
CSV_SOURCE = str(BASE_DIR / "food_delivery_apps.csv")


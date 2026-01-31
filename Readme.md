**SQL Query Builder & Data Pipeline**
A clean, reusable system for building SQL queries in Python, executing them against various data sources, and validating results with isolated testing.

🏗️ Architecture
Core Components
BaseQuery (sql/models/base_query.py) - SQL query builder with SELECT, WHERE, GROUP BY, ORDER BY

TableSpec (sql/models/table_spec.py) - Reusable table definition holding schema, source, and metadata

Registry (sql/registry/export_table.py) - Central TABLE_REGISTRY mapping table names to TableSpec instances

Cube Structure - Domain-specific folders containing schema, queries, and tests

**Project Structure**
s_Builder/
├── sql/                          # Core package
│   ├── models/                   # BaseQuery, TableSpec
│   ├── registry/                 # TABLE_REGISTRY
│   ├── cubes/                    # Domain-specific tables
│   │   └── food_review/         # Example cube
│   │       ├── food_review.py   # Domain queries
│   │       ├── schema.json      # Column definitions
│   │       └── test/            # Self-contained tests
│   └── tests/                   # Shared test utilities
├── scripts/                      # Migration & helpers
├── queries/                      # Example query files
└── config.py                     # Database paths


📁 File-by-File Implementation
1. Base Query Builder (sql/models/base_query.py)
2. Table Specification (sql/models/table_spec.py)
3. Registry (sql/registry/export_table.py)
4. Migration Script (scripts/migration_1.py)
5. Test Helper (scripts/helper.py)
6. Test Utilities (sql/tests/utils/utils.py)

🧪 Testing Pattern
Each cube has its own test folder with schema, mock data, ground truth

Tests use in-memory SQLite via create_test_table() generator

Mock data in CSV format loaded into test database

Queries executed against test data using same BaseQuery interface

Results compared with ground truth CSV using pandas assert_frame_equal

Auto-cleanup via generator's finally block


📄 File Manifest
sql/models/base_query.py          # Core query builder
sql/models/table_spec.py          # Table definition class
sql/registry/export_table.py      # Central table registry
sql/config.py                     # Database configuration
scripts/helper.py                 # Test database generator
scripts/migration_1.py            # CSV to SQLite migration
sql/tests/utils/utils.py          # Test path utilities


Each cube contains:
cubes/your_table/
├── your_table.py                 # Domain queries
├── schema.json                   # Column definitions
└── test/
    ├── test_your_table.py        # Test implementation
    ├── schema.json               # Test schema (copy)
    ├── mock_data.csv             # Test input data
    └── ground_truth.csv          # Expected results

Execution Flow
1. Define Schema → 2. Register Table → 3. Test Logic → 4. Migrate Data → 5. Execute Queries
       ↓                  ↓                  ↓             ↓                ↓
   schema.json      export_table.py     test files    (one-time)       production
                                          ✓           migration        queries

When to Migrate:
NEW TABLE        → Run migration (creates table + loads data)
ADD COLUMN       → Run migration (alters table + backfills)
DATA REFRESH     → Run migration (replaces/updates data)
TESTING ONLY     → No migration needed (in-memory)




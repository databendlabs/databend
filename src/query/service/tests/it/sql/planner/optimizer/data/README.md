# Optimizer Test Data

This directory contains test data for query optimizer tests. The tests are structured as follows:

## Directory Structure

```
data
├── tables/          # SQL table definitions
│   ├── basic/       # Basic table definitions
│   ├── tpcds/       # TPC-DS table definitions
│   └── obfuscated/  # Obfuscated table definitions
├── statistics/      # Statistics files
│   ├── basic/       # Basic statistics
│   ├── tpcds/       # TPC-DS statistics
│   └── obfuscated/  # Obfuscated statistics
├── cases/           # YAML test case definitions
│   ├── basic/       # Basic test cases
│   ├── tpcds/       # TPC-DS test cases
│   └── obfuscated/  # Obfuscated test cases
└── results/         # Test result files (generated)
    ├── basic/       # Results for basic test cases
    ├── tpcds/       # Results for TPC-DS test cases
    └── obfuscated/  # Results for obfuscated test cases
```

The test framework supports hierarchical subdirectory structures for better organization of test cases, tables, and results.

## YAML Test Case Format

Each test case is defined in a YAML file with the following structure:

```yaml
name: "Q3"                      # Test case name
description: "Test description" # Optional description

sql: |                          # SQL query to test
  SELECT ...

auto_statistics: false          # Whether to use CollectStatisticsOptimizer (default: false)

statistics_file: "tpcds_100g"   # Optional: reference external statistics file
                                # (from statistics/ directory, extension optional)

table_statistics:               # Inline table statistics (can be combined with statistics_file)
  table_name:
    num_rows: 1000
    data_size: 102400
    data_size_compressed: 51200
    index_size: 20480
    number_of_blocks: 10
    number_of_segments: 2

column_statistics:              # Inline column statistics (can be combined with statistics_file)
  table_name.column_name:
    min: 1990                   # Min value (can be number or string)
    max: 2000                   # Max value (can be number or string)
    ndv: 10                     # Number of distinct values
    null_count: 0               # Number of null values

good_plan: |                    # Optional expected good plan
  ...
```

## External Statistics Files

Statistics can be defined in separate YAML files in the `statistics/` directory:

```yaml
# statistics/tpcds/tpcds_100g.yaml
table_statistics:
  catalog_sales:
    num_rows: 143997065
    data_size: 12959733850
    # ... other stats
    
column_statistics:
  catalog_sales.cs_sold_date_sk:
    min: 2450815
    max: 2452921
    ndv: 1823
    null_count: 0
  # ... other columns
```

Test cases can reference these files using the `statistics_file` field. The framework will automatically search for matching files (with or without numeric prefixes like `01_tpcds_100g.yaml`).

## Table Definitions

Table definitions are stored in SQL files in the `tables/` directory. Each file contains `CREATE TABLE` statements. The framework will execute these SQL statements to set up the test environment. If a table already exists, the error will be ignored.

## Running Tests

### Run All Tests
```bash
cargo test --package databend-query --test it -- sql::planner::optimizer::optimizer_test::test_optimizer --exact --nocapture
```

### Run Tests from Specific Subdirectory
```bash
TEST_SUBDIR=tpcds cargo test --package databend-query --test it -- sql::planner::optimizer::optimizer_test::test_optimizer --exact --nocapture
```

## Generated Result Files

Each test case generates three result files in the corresponding subdirectory under `results/`:
- `{test_name}_raw.txt` - The raw plan before optimization
- `{test_name}_optimized.txt` - The optimized logical plan
- `{test_name}_physical.txt` - The physical execution plan

## Adding New Tests

To add a new test case:

1. Create a new YAML file in the appropriate subdirectory under `cases/` (e.g., `basic/`, `tpcds/`, or `obfuscated/`).
2. If the test uses new tables, add the table definitions to the corresponding subdirectory under `tables/`.
3. If needed, add statistics files to the corresponding subdirectory under `statistics/`.
4. The test runner will automatically discover and run all test cases recursively in all subdirectories.
5. Test results will be saved in a matching subdirectory structure under the main `results/` directory.

## Updating Existing Tests

If the expected output of a test changes (e.g., due to optimizer improvements):

1. Run the test with `UPDATE_GOLDENFILES` environment variable to generate new result files.
2. The new result files will be automatically saved in the correct subdirectory structure under the main `results/` directory.
3. Review the changes to ensure they are as expected.

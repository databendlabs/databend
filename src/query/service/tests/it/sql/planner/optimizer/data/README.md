# TPC-DS Optimizer Test Data

This directory contains test data for TPC-DS optimizer tests. The tests are structured as follows:

## Directory Structure

```
data
├── tables/          # SQL table definitions
│   ├── basic/       # Basic table definitions
│   ├── tpcds/       # TPC-DS table definitions
│   └── obfuscated/  # Obfuscated table definitions
├── statistics/      # Statistics files
├── cases/           # YAML test case definitions
│   ├── basic/       # Basic test cases
│   ├── tpcds/       # TPC-DS test cases
│   └── obfuscated/  # Obfuscated test cases
└── results/         # Test result files
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

table_statistics:               # Table statistics
  table_name:
    num_rows: 1000
    data_size: 102400
    data_size_compressed: 51200
    index_size: 20480
    number_of_blocks: 10
    number_of_segments: 2

column_statistics:              # Column statistics
  table_name.column_name:
    min: 1990                   # Min value (can be number or string)
    max: 2000                   # Max value (can be number or string)
    ndv: 10                     # Number of distinct values
    null_count: 0               # Number of null values

good_plan: |                    # Optional expected good plan
  ...
```

## Table Definitions

Table definitions are stored in SQL files in the `tables` directory. Each file contains a `CREATE TABLE` statement for a specific table used in the tests.

## Adding New Tests

To add a new test case:

1. Create a new YAML file in the appropriate subdirectory under `cases/` (e.g., `basic/`, `tpcds/`, or `obfuscated/`).
2. If the test uses new tables, add the table definitions to the corresponding subdirectory under `tables/`.
3. The test runner will automatically discover and run all test cases recursively in all subdirectories.
4. Test results will be saved in a matching subdirectory structure under the main `results/` directory (e.g., `results/basic/`, `results/tpcds/`, etc.).

## Updating Existing Tests

If the expected output of a test changes (e.g., due to optimizer improvements):

1. Run the test with UPDATE_GOLDENFILES to generate new result files.
2. The new result files will be automatically saved in the correct subdirectory structure under the main `results/` directory.
3. Review the changes to ensure they are as expected.

## Test Case Naming

When tests are organized in subdirectories, the test name displayed during test execution will include the subdirectory path as a prefix (e.g., `tpcds/Q01` instead of just `Q01`). This helps to identify which category a test belongs to when viewing test results.

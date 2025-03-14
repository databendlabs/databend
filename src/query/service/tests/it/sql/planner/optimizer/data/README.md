# TPC-DS Optimizer Test Data

This directory contains test data for TPC-DS optimizer tests. The tests are structured as follows:

## Directory Structure

```
data
├── tables/    # SQL table definitions
└── yaml/      # YAML test case definitions
```

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

raw_plan: |                     # Expected raw plan
  ...

optimized_plan: |               # Expected optimized plan
  ...

snow_plan: |                    # Optional expected snowflake plan
  ...
```

## Table Definitions

Table definitions are stored in SQL files in the `tables` directory. Each file contains a `CREATE TABLE` statement for a specific table used in the tests.

## Adding New Tests

To add a new test case:

1. Create a new YAML file in the `yaml` directory with the test case definition.
2. If the test uses new tables, add the table definitions to the `tables` directory.
3. The test runner will automatically discover and run all test cases in the `yaml` directory.

## Updating Existing Tests

If the expected output of a test changes (e.g., due to optimizer improvements):

1. Run the test to see the actual output.
2. Update the `raw_plan`, `optimized_plan`, or `snow_plan` field in the YAML file to match the actual output.

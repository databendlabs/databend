# TPC-DS Optimizer Test Data

This directory contains test data for TPC-DS optimizer tests. The tests are structured as follows:

## Directory Structure

```
data
├── tables/     # SQL table definitions
├── statistics/ # SQL table definitions
└── cases/      # YAML test case definitions and golden files
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

good_plan: |                    # Optional expected good plan
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

1. Run the test with UPDATE_GOLDENFILES to generate new file.
2. Checking that changes to files are as expected.

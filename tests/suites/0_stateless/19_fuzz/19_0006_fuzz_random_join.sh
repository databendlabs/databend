#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

run_bendsql() {
  cat <<SQL | $BENDSQL_CLIENT_CONNECT
$1
SQL
}

run_bendsql_null() {
  cat <<SQL | $BENDSQL_CLIENT_OUTPUT_NULL
$1
SQL
}

## Configuration
rows=1000
iterations=2
max_string_len=10
max_array_len=3

## Create test tables with specific columns
run_bendsql "
CREATE OR REPLACE TABLE join_fuzz1(
  id INT,
  int_col INT,
  str_col VARCHAR(50),
  bool_col BOOLEAN,
  float_col FLOAT,
  decimal_col DECIMAL(15,2),
  date_col DATE,
  time_col TIMESTAMP,
  array_col ARRAY(STRING),
  json_col VARIANT,
  null_col INT
);

CREATE OR REPLACE TABLE join_fuzz2 LIKE join_fuzz1;
CREATE OR REPLACE TABLE join_fuzz_r LIKE join_fuzz1 ENGINE = Random
  max_string_len = $max_string_len
  max_array_len = $max_array_len;
"

## Insert test data
run_bendsql_null "
INSERT INTO join_fuzz1 SELECT * FROM join_fuzz_r LIMIT $rows;
INSERT INTO join_fuzz2 SELECT * FROM join_fuzz_r LIMIT $rows;

-- Insert some NULLs explicitly
UPDATE join_fuzz1 SET null_col = NULL WHERE id % 10 = 0;
UPDATE join_fuzz2 SET null_col = NULL WHERE id % 7 = 0;

CREATE OR REPLACE TABLE empty_table LIKE join_fuzz1;
"

## List of actual columns in our tables
columns=("id" "int_col" "str_col" "bool_col" "float_col" "decimal_col" "date_col" "time_col" "null_col")

## Function to run a test and verify result
function run_test() {
  local query="$1"
  local expected="$2"
  local test_name="$3"

  result=$(echo "$query" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}')

  if [ "$result" != "$expected" ]; then
    echo "FAIL: $test_name (Expected $expected, got $result)"
    echo "Query: $query"
  fi
}

## Fuzz test different join types
for ((i=1; i<=$iterations; i++)); do
  echo "=== Iteration $i ==="

  # Randomly select actual columns to join on
  join_col=${columns[$((RANDOM % ${#columns[@]}))]}
  pick_col=${columns[$((RANDOM % ${#columns[@]}))]}

  # Random join condition type
  condition_type=$((RANDOM % 4))
  case $condition_type in
    0) condition="join_fuzz1.$join_col = join_fuzz2.$join_col";;
    1) condition="join_fuzz1.$join_col > join_fuzz2.$join_col";;
    2) condition="join_fuzz1.$join_col <= join_fuzz2.$join_col";;
    3) condition="join_fuzz1.$join_col != join_fuzz2.$join_col";;
  esac

  # Test INNER JOIN properties, may have n ---> n
  run_test """
        SELECT (
        SELECT COUNT(*) FROM (
        SELECT join_fuzz1.id, join_fuzz2.id, COUNT(*) AS cnt
        FROM join_fuzz1 INNER JOIN join_fuzz2 ON $condition
        GROUP BY join_fuzz1.id, join_fuzz2.id
        EXCEPT
        SELECT join_fuzz1.id, join_fuzz2.id, COUNT(*) AS cnt
        FROM join_fuzz2 INNER JOIN join_fuzz1 ON $condition
        GROUP BY join_fuzz2.id, join_fuzz1.id
        )
        ) = 0;
        """ "true" "INNER JOIN commutative property (with cardinality)"

  # Test LEFT JOIN properties
  run_test """
    SELECT COUNT(*) FROM join_fuzz1
    WHERE NOT EXISTS (
      SELECT 1 FROM join_fuzz1 LEFT JOIN join_fuzz2 ON $condition
      WHERE join_fuzz1.id = join_fuzz1.id
    );
  """ "0" "LEFT JOIN preserves all left table rows"

  run_test """
    SELECT COUNT(*) FROM join_fuzz1 LEFT JOIN join_fuzz2 ON $condition
    WHERE join_fuzz1.id NOT IN (SELECT id FROM join_fuzz2)
    AND join_fuzz2.id IS NOT NULL;
  """ "0" "LEFT JOIN NULL check for non-matching rows"

  # Test RIGHT JOIN properties
  run_test """
    SELECT COUNT(*) FROM join_fuzz2
    WHERE NOT EXISTS (
      SELECT 1 FROM join_fuzz1 RIGHT JOIN join_fuzz2 ON $condition
      WHERE join_fuzz2.id = join_fuzz2.id
    );
  """ "0" "RIGHT JOIN preserves all right table rows"

  run_test """
    SELECT COUNT(*) FROM join_fuzz1 RIGHT JOIN join_fuzz2 ON $condition
    WHERE join_fuzz2.id NOT IN (SELECT id FROM join_fuzz1)
    AND join_fuzz1.id IS NOT NULL;
  """ "0" "RIGHT JOIN NULL check for non-matching rows"

  # Test FULL OUTER JOIN properties
  run_test """
    SELECT COUNT(*) FROM (
      (SELECT join_fuzz1.*, join_fuzz2.* FROM join_fuzz1 FULL OUTER JOIN join_fuzz2 ON $condition)
      EXCEPT
      (SELECT join_fuzz1.*, join_fuzz2.* FROM join_fuzz2 FULL OUTER JOIN join_fuzz1 ON $condition)
    );
  """ "0" "FULL OUTER JOIN symmetry check"

  run_test """
    SELECT (SELECT COUNT(*) FROM join_fuzz1 FULL OUTER JOIN join_fuzz2 ON $condition) =
           ((SELECT COUNT(*) FROM join_fuzz1 LEFT JOIN join_fuzz2 ON $condition) +
            (SELECT COUNT(*) FROM join_fuzz1 RIGHT JOIN join_fuzz2 ON $condition) -
            (SELECT COUNT(*) FROM join_fuzz1 INNER JOIN join_fuzz2 ON $condition));
  """ "true" "FULL OUTER JOIN = LEFT + RIGHT - INNER"

  # Test CROSS JOIN
  run_test """
    SELECT (SELECT COUNT(*) FROM join_fuzz1) * (SELECT COUNT(*) FROM join_fuzz2) =
           (SELECT COUNT(*) FROM join_fuzz1 CROSS JOIN join_fuzz2);
  """ "true" "CROSS JOIN produces Cartesian product"

  # Test NULL handling
  run_test """
    SELECT COUNT(*) FROM join_fuzz1 INNER JOIN join_fuzz2
    ON join_fuzz1.null_col = join_fuzz2.null_col
    WHERE join_fuzz1.null_col IS NULL OR join_fuzz2.null_col IS NULL;
  """ "0" "INNER JOIN with NULL keys returns no rows"

  # Test empty table joins
  run_test """
    SELECT COUNT(*) FROM empty_table as join_fuzz1 JOIN join_fuzz2 ON $condition;
  """ "0" "JOIN with empty table returns no rows"

  run_test """
    SELECT COUNT(*) FROM join_fuzz1 LEFT JOIN empty_table as join_fuzz2  ON $condition;
  """ "$(echo "SELECT COUNT(*) FROM join_fuzz1;" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}')" "LEFT JOIN with empty table preserves left table"

  # Test type compatibility
  run_test """
    SELECT COUNT(*) FROM (
      (SELECT * FROM join_fuzz1 JOIN join_fuzz2 ON TRY_CAST(join_fuzz1.int_col AS VARCHAR) = join_fuzz2.str_col)
      EXCEPT
      (SELECT * FROM join_fuzz1 JOIN join_fuzz2 ON TRY_CAST(join_fuzz1.int_col AS VARCHAR) = join_fuzz2.str_col)
    );
  """ "0" "Implicit type conversion in JOIN condition"
done

## Clean up
# echo """
# DROP TABLE if exists join_fuzz1;
# DROP TABLE if exists join_fuzz2;
# DROP TABLE if exists join_fuzz_r;
# DROP TABLE if exists empty_table;
# """ | $BENDSQL_CLIENT_CONNECT

#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists sample_table" | bendsql_connect_root

## Create table
cat <<EOF | bendsql_connect_root
CREATE TABLE sample_table
(
    Id     INT,
    City2  VARCHAR AS (reverse(City)) STORED,
    Score2 INT64 AS (Score + 2) VIRTUAL,
    City   VARCHAR,
    Score  INT
);
EOF

copy_from_test_csv=(
  "copy into sample_table from 'fs://${TESTS_DATA_DIR}/csv/sample.csv' FILE_FORMAT = (field_delimiter = ',' record_delimiter = '\n' type = CSV) ON_ERROR = ABORT"
)

echo "---test csv field with computed columns"
for i in "${copy_from_test_csv[@]}"; do
  echo "$i" | bendsql_connect_root
  echo "select * from sample_table" | bendsql_connect_root
  echo "truncate table sample_table" | bendsql_connect_root
done

## Drop table
echo "drop table if exists sample_table;" | bendsql_connect_root

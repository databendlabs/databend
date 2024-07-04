#!/bin/bash
## uncomment these when QUERY_DATABEND_ENTERPRISE_LICENSE env is set 

# cat <<SQL | ${BENDSQL}
# CREATE DATABASE IF NOT EXISTS stream_test;

# CREATE OR REPLACE TABLE stream_test.abc
# (
#     title VARCHAR,
#     author VARCHAR,
#     date VARCHAR
# );

# CREATE OR REPLACE STREAM stream_test.s on table stream_test.abc;
# SQL

# cat <<SQL | ${BENDSQL} -D stream_test
# DROP TABLE abc
# SQL

cat <<SQL | ${BENDSQL} -D stream_test
select 1;
SQL

cat <<SQL | ${BENDSQL}
DROP DATABASE IF EXISTS stream_test;
SQL

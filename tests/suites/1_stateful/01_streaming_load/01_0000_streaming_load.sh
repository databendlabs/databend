#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "drop table if exists ontime_streaming_load;" | $BENDSQL_CLIENT_CONNECT
## create ontime table
cat $TESTS_DATA_DIR/ddl/ontime.sql | sed 's/ontime/ontime_streaming_load/g' | $BENDSQL_CLIENT_CONNECT

# load csv
echo "--csv"
curl -H "enable_streaming_load:1" -H "insert_sql:insert into ontime_streaming_load file_format = (type = CSV skip_header = 1)" -F "upload=@/${TESTS_DATA_DIR}/ontime_200.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime_streaming_load;" | $BENDSQL_CLIENT_CONNECT
echo "truncate table ontime_streaming_load" | $BENDSQL_CLIENT_CONNECT

echo "--csv.gz"
# load csv gz
curl -H "enable_streaming_load:1" -H "insert_sql:insert into ontime_streaming_load file_format = (type = CSV skip_header = 1 compression = 'gzip')" -F "upload=@/${TESTS_DATA_DIR}/ontime_200.csv.gz" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime_streaming_load;" | $BENDSQL_CLIENT_CONNECT
echo "truncate table ontime_streaming_load" | $BENDSQL_CLIENT_CONNECT

# load csv zstd
echo "--csv.zstd"
curl -H "enable_streaming_load:1" -H "insert_sql:insert into ontime_streaming_load file_format = (type = CSV skip_header = 1 compression = 'zstd')" -F  "upload=@/${TESTS_DATA_DIR}/ontime_200.csv.zst" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime_streaming_load;" | $BENDSQL_CLIENT_CONNECT
echo "truncate table ontime_streaming_load" | $BENDSQL_CLIENT_CONNECT

# load csv bz2
echo "--csv.bz2"
curl -H "enable_streaming_load:1" -H "insert_sql:insert into ontime_streaming_load file_format = (type = CSV skip_header = 1 compression = 'bz2')" -F  "upload=@/${TESTS_DATA_DIR}/ontime_200.csv.bz2" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime_streaming_load;" | $BENDSQL_CLIENT_CONNECT
echo "truncate table ontime_streaming_load" | $BENDSQL_CLIENT_CONNECT

# load parquet
echo "--parquet"
curl -H "enable_streaming_load:1" -H "insert_sql:insert into ontime_streaming_load file_format = (type = Parquet)" -F "upload=@/${TESTS_DATA_DIR}/ontime_200.parquet" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime_streaming_load;" | $BENDSQL_CLIENT_CONNECT
echo "truncate table ontime_streaming_load" | $BENDSQL_CLIENT_CONNECT

# load ndjson
echo "--ndjson"
curl -H "enable_streaming_load:1" -H "insert_sql:insert into ontime_streaming_load file_format = (type = NdJson)" -F "upload=@/${TESTS_DATA_DIR}/ontime_200.ndjson" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime_streaming_load;" | $BENDSQL_CLIENT_CONNECT
echo "truncate table ontime_streaming_load" | $BENDSQL_CLIENT_CONNECT

# load parquet with less schema
echo 'CREATE TABLE ontime_less
(
    Year                            SMALLINT UNSIGNED,
    Quarter                         TINYINT UNSIGNED,
    Month                           TINYINT UNSIGNED,
    DayofMonth                      TINYINT UNSIGNED,
    DayOfWeek                       TINYINT UNSIGNED
)' | $BENDSQL_CLIENT_CONNECT


echo "--parquet less"
curl -s -H "enable_streaming_load:1" -H "insert_sql:insert into ontime_less file_format = (type = Parquet)" -F "upload=@/${TESTS_DATA_DIR}/ontime_200.parquet" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load"  > /dev/null 2>&1
echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime_less;" | $BENDSQL_CLIENT_CONNECT

# load parquet with mismatch schema, will auto cast
echo "--parquet runtime cast schema"
cat $TESTS_DATA_DIR/ddl/ontime.sql | sed 's/ontime/ontime_test_schema_mismatch/g' | sed 's/DATE/TIMESTAMP/g' | $BENDSQL_CLIENT_CONNECT
curl -s -H "enable_streaming_load:1" -H "insert_sql:insert into ontime_test_schema_mismatch file_format = (type = Parquet)" -F "upload=@/${TESTS_DATA_DIR}/ontime_200.parquet" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load"  > /dev/null 2>&1
echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime_test_schema_mismatch;" | $BENDSQL_CLIENT_CONNECT


echo "drop table ontime_streaming_load;" | $BENDSQL_CLIENT_CONNECT
echo "drop table ontime_test_schema_mismatch;" | $BENDSQL_CLIENT_CONNECT
echo "drop table ontime_less;" | $BENDSQL_CLIENT_CONNECT

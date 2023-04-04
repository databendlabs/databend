#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists wrong_csv;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists wrong_ndjson" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists wrong_tsv" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists wrong_xml" | $MYSQL_CLIENT_CONNECT

## Create table
cat $CURDIR/../ddl/on_error_test.sql | $MYSQL_CLIENT_CONNECT

# Should be <root>/tests/data/
DATADIR=$(realpath $CURDIR/../../../data/)

# This line is kept for debugging.
# echo "Current data dir: ${DATADIR}"

# copy wrong files on_error=continue
WRONG_CSV="COPY INTO wrong_csv FROM 'fs://${DATADIR}/wrong_sample.csv' FILE_FORMAT = (type = CSV field_delimiter = ','  record_delimiter = '\n' skip_header = 0) ON_ERROR=continue"

echo "$WRONG_CSV" | $MYSQL_CLIENT_CONNECT
echo "select count(1) from wrong_csv" | $MYSQL_CLIENT_CONNECT
echo "truncate table wrong_csv" | $MYSQL_CLIENT_CONNECT

WRONG_NDJSON="COPY INTO wrong_ndjson FROM 'fs://${DATADIR}/wrong_json_sample.ndjson' FILE_FORMAT = (type = ndjson) ON_ERROR=continue"

echo "$WRONG_NDJSON" | $MYSQL_CLIENT_CONNECT
echo "select count(1) from wrong_ndjson" | $MYSQL_CLIENT_CONNECT
echo "truncate table wrong_ndjson" | $MYSQL_CLIENT_CONNECT

WRONG_TSV="COPY INTO wrong_tsv FROM 'fs://${DATADIR}/wrong_tsv_sample.tsv' FILE_FORMAT = (type = TSV) ON_ERROR=continue"

echo "$WRONG_TSV" | $MYSQL_CLIENT_CONNECT
echo "select count(1) from wrong_tsv" | $MYSQL_CLIENT_CONNECT
echo "truncate table wrong_tsv" | $MYSQL_CLIENT_CONNECT

WRONG_XML="COPY INTO wrong_xml FROM 'fs://${DATADIR}/wrong_xml_sample.xml' FILE_FORMAT = (type = xml) ON_ERROR=continue;"

echo "$WRONG_XML" | $MYSQL_CLIENT_CONNECT
echo "select count(1) from wrong_xml" | $MYSQL_CLIENT_CONNECT
echo "truncate table wrong_xml" | $MYSQL_CLIENT_CONNECT

# copy wrong files on_error=abort_n
WRONG_CSV="COPY INTO wrong_csv FROM 'fs://${DATADIR}/wrong_sample.csv' FILE_FORMAT = (type = CSV field_delimiter = ','  record_delimiter = '\n' skip_header = 0) ON_ERROR=abort_3"

echo "$WRONG_CSV" | $MYSQL_CLIENT_CONNECT
echo "select count(1) from wrong_csv" | $MYSQL_CLIENT_CONNECT
echo "truncate table wrong_csv" | $MYSQL_CLIENT_CONNECT

WRONG_CSV="COPY INTO wrong_csv FROM 'fs://${DATADIR}/wrong_sample.csv' FILE_FORMAT = (type = CSV field_delimiter = ','  record_delimiter = '\n' skip_header = 0) ON_ERROR=abort_2"

echo "$WRONG_CSV" | $MYSQL_CLIENT_CONNECT 2>&1 | grep -c "fail to decode column"
echo "select count(1) from wrong_csv" | $MYSQL_CLIENT_CONNECT
echo "truncate table wrong_csv" | $MYSQL_CLIENT_CONNECT

## Drop table
echo "drop table if exists wrong_csv;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists wrong_ndjson" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists wrong_tsv" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists wrong_xml" | $MYSQL_CLIENT_CONNECT

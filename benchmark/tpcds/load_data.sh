#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/shell_env.sh

factor=$1

echo """
INSTALL tpcds;
LOAD tpcds;
SELECT * FROM dsdgen(sf=$factor); -- sf can be other values, such as 0.1, 1, 10, ...
EXPORT DATABASE '/tmp/tpcds_$factor/' (FORMAT CSV, DELIMITER '|');
""" | duckdb

mv /tmp/tpcds_$factor/ "$(pwd)/data/"

# Create Database
echo "CREATE DATABASE IF NOT EXISTS ${MYSQL_DATABASE}" | $BENDSQL_CLIENT_CONNECT_DEFAULT

tables=(
    call_center
    catalog_returns
    customer_address
    customer_demographics
    household_demographics
    inventory
    promotion
    ship_mode
    store_returns
    time_dim
    web_page
    web_sales
    catalog_page
    catalog_sales
    customer
    date_dim
    income_band
    item
    reason
    store
    store_sales
    warehouse
    web_returns
    web_site
)

# Clear Data
for t in ${tables[@]}
do
    echo "DROP TABLE IF EXISTS $t ALL" | $BENDSQL_CLIENT_CONNECT
done

# Create Tables;
cat "$CURDIR"/tpcds.sql | $BENDSQL_CLIENT_CONNECT

# Load Data
# note: export STORAGE_ALLOW_INSECURE=true to start databend-query
for t in ${tables[@]}
do
    echo "$t"
    fp="`pwd`/data/$t.csv"
    echo "copy into ${MYSQL_DATABASE}.$t from 'fs://${fp}' file_format = (type = CSV skip_header = 1 field_delimiter = '|' record_delimiter = '\n')" | $BENDSQL_CLIENT_CONNECT
    echo "analyze table ${MYSQL_DATABASE}.$t" | $BENDSQL_CLIENT_CONNECT
done



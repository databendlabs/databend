#!/usr/bin/env bash

. tests/shell_env.sh

# shellcheck disable=SC2034
target_dir="tests/sqllogictests/"

db="tpcds"

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
# shellcheck disable=SC2068
for t in ${tables[@]}; do
    echo "DROP TABLE IF EXISTS ${db}.$t" | $BENDSQL_CLIENT_CONNECT
done

echo "CREATE DATABASE IF NOT EXISTS tpcds" | $BENDSQL_CLIENT_CONNECT

# Create Tables;
# shellcheck disable=SC2002
cat ${target_dir}/scripts/tpcds.sql | $BENDSQL_CLIENT_CONNECT

# download data
mkdir -p ${target_dir}/data/
if [ ! -d ${target_dir}/data/tpcds.tar.gz ]; then
    curl -s -o ${target_dir}/data/tpcds.tar.gz https://ci.databend.org/dataset/stateful/tpcds.tar.gz
fi

tar -zxf ${target_dir}/data/tpcds.tar.gz -C ${target_dir}/data

# insert data to tables
# shellcheck disable=SC2068
for t in ${tables[@]}; do
    echo $t
    insert_sql="insert into ${db}.$t file_format = (type = CSV skip_header = 0 field_delimiter = '|' record_delimiter = '\n')"
    curl -s -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" -H "insert_sql: ${insert_sql}" -F 'upload=@"'${target_dir}'/data/data/'$t'.csv"' >/dev/null 2>&1
    echo "analyze table $db.$t" | $BENDSQL_CLIENT_CONNECT
done

if [ -d "tests/sqllogictests/data" ]; then
    rm -rf tests/sqllogictests/data
fi

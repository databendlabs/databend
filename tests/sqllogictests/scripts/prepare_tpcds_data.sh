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

stmt "drop stage if exists s1"
stmt "create stage s1 url='fs://${PWD}/${target_dir}/'"

for t in ${tables[@]}; do
    echo "$t"
    sub_path="data/data/$t.csv"
   	query "copy into ${db}.${t} from @s1/${sub_path} file_format = (type = CSV skip_header = 0 field_delimiter = '|' record_delimiter = '\n')"
    query "analyze table $db.$t"
done

if [ -d "tests/sqllogictests/data" ]; then
    rm -rf tests/sqllogictests/data
fi

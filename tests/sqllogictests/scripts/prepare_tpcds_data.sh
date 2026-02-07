#!/usr/bin/env bash

. tests/shell_env.sh

# shellcheck disable=SC2034
target_dir="tests/sqllogictests/"

db=${1:-"tpcds"}


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


force=${2:-"1"}
if [ "$force" == "0" ]; then
    table_exists=$(echo "SELECT COUNT() FROM system.tables WHERE database = '${db}' AND name = 'call_center'" | $BENDSQL_CLIENT_CONNECT --output tsv --quote-style never)
    table_exists=$(echo "$table_exists" | tr -d '\r\n[:space:]')
    if [ -n "$table_exists" ] && [ "$table_exists" -gt 0 ]; then
        res=$(echo "SELECT COUNT() from ${db}.call_center" | $BENDSQL_CLIENT_CONNECT --output tsv --quote-style never)
        res=$(echo "$res" | tr -d '\r\n[:space:]')
        if [ -n "$res" ] && [ "$res" -gt 0 ]; then
            echo "Table $db.call_center already exists and is not empty, size: ${res}. Use force=1 to override it."
            exit 0
        fi
    fi
fi

echo "CREATE OR REPLACE DATABASE tpcds" | $BENDSQL_CLIENT_CONNECT

# Create Tables;
# shellcheck disable=SC2002
cat ${target_dir}/scripts/tpcds.sql | $BENDSQL_CLIENT_CONNECT
python ${target_dir}/scripts/prepare_duckdb_tpcds_data.py 1

stmt "drop stage if exists s1"
stmt "create stage s1 url='fs:///tmp/tpcds_1/'"

for t in ${tables[@]}; do
    echo "$t"
   	query "copy into ${db}.${t} from @s1/${t}.csv file_format = (type = CSV skip_header = 1 field_delimiter = '|' record_delimiter = '\n')"
    query "analyze table $db.$t"
done

# rm -rf /tmp/tpcds_1

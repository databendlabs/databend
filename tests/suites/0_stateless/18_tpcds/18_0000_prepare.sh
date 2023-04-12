#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

db="default"

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
for t in ${tables[@]}
do
    echo "DROP TABLE IF EXISTS $db.$t ALL" | $MYSQL_CLIENT_CONNECT
done

# Create Tables;
# shellcheck disable=SC2002
cat ${CURDIR}/prepare/tpcds.sql | $MYSQL_CLIENT_CONNECT

# download data
mkdir -p  ${CURDIR}/data/
if [ ! -d ${CURDIR}/data/tpcds.tar.gz ];then
    curl -s -o ${CURDIR}/data/tpcds.tar.gz  http://repo.databend.rs/dataset/stateful/tpcds.tar.gz
fi

tar -zxf ${CURDIR}/data/tpcds.tar.gz -C ${CURDIR}/data

# insert data to tables
# shellcheck disable=SC2068
for t in ${tables[@]}
do
    echo $t
    insert_sql="insert into ${db}.$t file_format = (type = CSV skip_header = 0 field_delimiter = '|' record_delimiter = '\n')"
    curl -s -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" -H "insert_sql: ${insert_sql}" -F 'upload=@"'${CURDIR}'/data/data/'$t'.csv"' > /dev/null 2>&1
done


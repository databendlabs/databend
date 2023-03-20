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
    echo "DROP TABLE IF EXISTS $t ALL" | $MYSQL_CLIENT_CONNECT
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
  echo "copy into $t from 'fs://${CURDIR}/data/data/${t}/${t}.parquet' FILE_FORMAT = (type = PARQUET)" | $MYSQL_CLIENT_CONNECT
done


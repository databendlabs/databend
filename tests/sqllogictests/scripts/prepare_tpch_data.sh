#!/usr/bin/env bash

. tests/shell_env.sh

# shellcheck disable=SC2034
data_dir="tests/sqllogictests/data"

db=${1:-"tpch_test"}
force=${2:-"1"}

if [ "$force" == "0" ]; then
    table_exists=$(echo "SELECT COUNT() FROM system.tables WHERE database = '${db}' AND name = 'nation'" | $BENDSQL_CLIENT_CONNECT --output tsv --quote-style never)
    table_exists=$(echo "$table_exists" | tr -d '\r\n[:space:]')
    if [ -n "$table_exists" ] && [ "$table_exists" -gt 0 ]; then
        res=$(echo "SELECT COUNT() from ${db}.nation" | $BENDSQL_CLIENT_CONNECT --output tsv --quote-style never)
        res=$(echo "$res" | tr -d '\r\n[:space:]')
        if [ -n "$res" ] && [ "$res" -gt 0 ]; then
            echo "Table $db.nation already exists and is not empty, size: ${res}. Use force=1 to override it."
            exit 0
        fi
    fi
fi

echo "DROP DATABASE if EXISTS ${db}" | $BENDSQL_CLIENT_CONNECT
echo "CREATE DATABASE ${db}" | $BENDSQL_CLIENT_CONNECT

for t in customer lineitem nation orders partsupp part region supplier; do
    echo "DROP TABLE IF EXISTS ${db}.$t" | $BENDSQL_CLIENT_CONNECT
done

# create tpch tables
echo "CREATE TABLE IF NOT EXISTS ${db}.nation
(
    n_nationkey  INTEGER not null,
    n_name       STRING not null,
    n_regionkey  INTEGER not null,
    n_comment    STRING
) CLUSTER BY (n_nationkey)" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS ${db}.region
(
    r_regionkey  INTEGER not null,
    r_name       STRING not null,
    r_comment    STRING
) CLUSTER BY (r_regionkey)" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS ${db}.part
(
    p_partkey     BIGINT not null,
    p_name        STRING not null,
    p_mfgr        STRING not null,
    p_brand       STRING not null,
    p_type        STRING not null,
    p_size        INTEGER not null,
    p_container   STRING not null,
    p_retailprice DECIMAL(15, 2) not null,
    p_comment     STRING not null
) CLUSTER BY (p_partkey)" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS ${db}.supplier
(
    s_suppkey     BIGINT not null,
    s_name        STRING not null,
    s_address     STRING not null,
    s_nationkey   INTEGER not null,
    s_phone       STRING not null,
    s_acctbal     DECIMAL(15, 2) not null,
    s_comment     STRING not null
) CLUSTER BY (s_suppkey)" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS ${db}.partsupp
(
    ps_partkey     BIGINT not null,
    ps_suppkey     BIGINT not null,
    ps_availqty    BIGINT not null,
    ps_supplycost  DECIMAL(15, 2)  not null,
    ps_comment     STRING not null
) CLUSTER BY (ps_partkey)" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS ${db}.customer
(
    c_custkey     BIGINT not null,
    c_name        STRING not null,
    c_address     STRING not null,
    c_nationkey   INTEGER not null,
    c_phone       STRING not null,
    c_acctbal     DECIMAL(15, 2)   not null,
    c_mktsegment  STRING not null,
    c_comment     STRING not null
) CLUSTER BY (c_custkey)" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS ${db}.orders
(
    o_orderkey       BIGINT not null,
    o_custkey        BIGINT not null,
    o_orderstatus    STRING not null,
    o_totalprice     DECIMAL(15, 2) not null,
    o_orderdate      DATE not null,
    o_orderpriority  STRING not null,
    o_clerk          STRING not null,
    o_shippriority   INTEGER not null,
    o_comment        STRING not null
) CLUSTER BY (o_orderkey, o_orderdate)" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS ${db}.lineitem
(
    l_orderkey    BIGINT not null,
    l_partkey     BIGINT not null,
    l_suppkey     BIGINT not null,
    l_linenumber  BIGINT not null,
    l_quantity    DECIMAL(15, 2) not null,
    l_extendedprice  DECIMAL(15, 2) not null,
    l_discount    DECIMAL(15, 2) not null,
    l_tax         DECIMAL(15, 2) not null,
    l_returnflag  STRING not null,
    l_linestatus  STRING not null,
    l_shipdate    DATE not null,
    l_commitdate  DATE not null,
    l_receiptdate DATE not null,
    l_shipinstruct STRING not null,
    l_shipmode     STRING not null,
    l_comment      STRING not null
) CLUSTER BY(l_shipdate, l_orderkey)" | $BENDSQL_CLIENT_CONNECT

#import data
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
python $CURDIR/prepare_duckdb_tpch_data.py 1
ls -lh /tmp/tpch_1/*

stmt "drop stage if exists s1"
stmt "create stage s1 url='fs:///tmp/tpch_1/'"

# insert data to tables
for t in customer lineitem nation orders partsupp part region supplier; do
    echo "$t"
   	query "copy into ${db}.$t from @s1/$t.csv force = true file_format = (type = CSV skip_header = 1 field_delimiter = '|' record_delimiter = '\n')"
    query "analyze table $db.$t"
done


# rm -rf /tmp/tpch_1

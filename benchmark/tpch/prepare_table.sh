#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/shell_env.sh


options="$1"

# Create Database
echo "CREATE DATABASE IF NOT EXISTS ${MYSQL_DATABASE}" | $BENDSQL_CLIENT_CONNECT_DEFAULT

echo "use ${MYSQL_DATABASE}" | $BENDSQL_CLIENT_CONNECT_DEFAULT

for t in customer lineitem nation orders partsupp part region supplier; do
    echo "DROP TABLE IF EXISTS $t" | $BENDSQL_CLIENT_CONNECT
done


# create tpch tables
echo "CREATE TABLE IF NOT EXISTS nation
(
    n_nationkey  INTEGER not null,
    n_name       STRING not null,
    n_regionkey  INTEGER not null,
    n_comment    STRING
) CLUSTER BY (n_nationkey) ${options}" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS region
(
    r_regionkey  INTEGER not null,
    r_name       STRING not null,
    r_comment    STRING
) CLUSTER BY (r_regionkey) ${options}" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS part
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
) CLUSTER BY (p_partkey) ${options}" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS supplier
(
    s_suppkey     BIGINT not null,
    s_name        STRING not null,
    s_address     STRING not null,
    s_nationkey   INTEGER not null,
    s_phone       STRING not null,
    s_acctbal     DECIMAL(15, 2) not null,
    s_comment     STRING not null
) CLUSTER BY (s_suppkey) ${options}" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS partsupp
(
    ps_partkey     BIGINT not null,
    ps_suppkey     BIGINT not null,
    ps_availqty    BIGINT not null,
    ps_supplycost  DECIMAL(15, 2)  not null,
    ps_comment     STRING not null
) CLUSTER BY (ps_partkey) ${options}" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS customer
(
    c_custkey     BIGINT not null,
    c_name        STRING not null,
    c_address     STRING not null,
    c_nationkey   INTEGER not null,
    c_phone       STRING not null,
    c_acctbal     DECIMAL(15, 2)   not null,
    c_mktsegment  STRING not null,
    c_comment     STRING not null
) CLUSTER BY (c_custkey) ${options}" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS orders
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
) CLUSTER BY (o_orderkey, o_orderdate) ${options}" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS lineitem
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
) CLUSTER BY(l_shipdate, l_orderkey) ${options}" | $BENDSQL_CLIENT_CONNECT

# insert data to tables
for t in customer lineitem nation orders partsupp part region supplier
do
    echo "$t"
    insert_sql="insert into ${MYSQL_DATABASE}.$t file_format = (type = CSV skip_header = 0 field_delimiter = '|' record_delimiter = '\n')"
    curl -s -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" -H "database: tpch" -H "insert_sql: ${insert_sql}" -F 'upload=@"./data/'$t'.tbl"'
done

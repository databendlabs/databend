#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


# create tpch tables
echo "CREATE TABLE IF NOT EXISTS nation
(
    n_nationkey  INTEGER not null,
    n_name       STRING not null,
    n_regionkey  INTEGER not null,
    n_comment    STRING
)" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS region
(
    r_regionkey  INTEGER not null,
    r_name       STRING not null,
    r_comment    STRING
)" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS part
(
    p_partkey     BIGINT not null,
    p_name        STRING not null,
    p_mfgr        STRING not null,
    p_brand       STRING not null,
    p_type        STRING not null,
    p_size        INTEGER not null,
    p_container   STRING not null,
    p_retailprice DOUBLE not null,
    p_comment     STRING not null
)" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS supplier
(
    s_suppkey     BIGINT not null,
    s_name        STRING not null,
    s_address     STRING not null,
    s_nationkey   INTEGER not null,
    s_phone       STRING not null,
    s_acctbal     DOUBLE not null,
    s_comment     STRING not null
)" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS partsupp
(
    ps_partkey     BIGINT not null,
    ps_suppkey     BIGINT not null,
    ps_availqty    BIGINT not null,
    ps_supplycost  DOUBLE  not null,
    ps_comment     STRING not null
)" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS customer
(
    c_custkey     BIGINT not null,
    c_name        STRING not null,
    c_address     STRING not null,
    c_nationkey   INTEGER not null,
    c_phone       STRING not null,
    c_acctbal     DOUBLE   not null,
    c_mktsegment  STRING not null,
    c_comment     STRING not null
)" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS orders
(
    o_orderkey       BIGINT not null,
    o_custkey        BIGINT not null,
    o_orderstatus    STRING not null,
    o_totalprice     DOUBLE not null,
    o_orderdate      DATE not null,
    o_orderpriority  STRING not null,
    o_clerk          STRING not null,
    o_shippriority   INTEGER not null,
    o_comment        STRING not null
)" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS lineitem
(
    l_orderkey    BIGINT not null,
    l_partkey     BIGINT not null,
    l_suppkey     BIGINT not null,
    l_linenumber  BIGINT not null,
    l_quantity    DOUBLE not null,
    l_extendedprice  DOUBLE not null,
    l_discount    DOUBLE not null,
    l_tax         DOUBLE not null,
    l_returnflag  STRING not null,
    l_linestatus  STRING not null,
    l_shipdate    DATE not null,
    l_commitdate  DATE not null,
    l_receiptdate DATE not null,
    l_shipinstruct STRING not null,
    l_shipmode     STRING not null,
    l_comment      STRING not null
)" | $MYSQL_CLIENT_CONNECT


#download data
mkdir -p  ${CURDIR}/data/
if [ ! -d ${CURDIR}/data/tpch.tar.gz ];then
    curl -s -o ${CURDIR}/data/tpch.tar.gz  http://repo.databend.rs/dataset/stateful/tpch.tar.gz
fi

tar -zxf ${CURDIR}/data/tpch.tar.gz -C ${CURDIR}/data

# insert data to tables
for t in customer lineitem nation orders partsupp part region supplier
do
    echo "$t"
    curl -s -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" -H 'insert_sql: insert into '$t' format CSV' -H 'skip_header: 0' -H 'field_delimiter:|' -H 'record_delimiter: \n' -F 'upload=@"'${CURDIR}'/data/tests/suites/0_stateless/13_tpch/data/'$t'.tbl"' > /dev/null 2>&1
done

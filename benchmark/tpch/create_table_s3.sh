#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../tests/shell_env.sh


for t in customer lineitem nation orders partsupp part region supplier; do
    echo "DROP TABLE IF EXISTS $t" | $MYSQL_CLIENT_CONNECT
done

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

# copy data to s3 bucket

# create external stage
echo"
create stage tpch_data url='s3://...'
  connection = (
        ENDPOINT_URL = 'https://s3.amazonaws.com'
        ACCESS_KEY_ID = '...'
        SECRET_ACCESS_KEY = '...'
        ENABLE_VIRTUAL_HOST_STYLE = 'false'
  );
" | $MYSQL_CLIENT_CONNECT

echo "list @tpch_data;" |  $MYSQL_CLIENT_CONNECT

# copy data to table
echo"
copy into orders from @tpch_data files=('orders.tbl') file_format=(type='CSV' field_delimiter='|' record_delimiter='\n' compression=auto);
" | $MYSQL_CLIENT_CONNECT

echo"
copy into nation from @tpch_data files=('nation.tbl') file_format=(type='CSV' field_delimiter='|' record_delimiter='\n' compression=auto);
" | $MYSQL_CLIENT_CONNECT

echo"
copy into lineitem from @tpch_data files=('lineitem.tbl') file_format=(type='CSV' field_delimiter='|' record_delimiter='\n' compression=auto);
" | $MYSQL_CLIENT_CONNECT

echo"
copy into customer from @tpch_data files=('customer.tbl') file_format=(type='CSV' field_delimiter='|' record_delimiter='\n' compression=auto);
" | $MYSQL_CLIENT_CONNECT

echo"
copy into partsupp from @tpch_data files=('partsupp.tbl') file_format=(type='CSV' field_delimiter='|' record_delimiter='\n' compression=auto);
" | $MYSQL_CLIENT_CONNECT

echo"
copy into supplier from @tpch_data files=('supplier.tbl') file_format=(type='CSV' field_delimiter='|' record_delimiter='\n' compression=auto);
" | $MYSQL_CLIENT_CONNECT

echo"
copy into part from @tpch_data files=('part.tbl') file_format=(type='CSV' field_delimiter='|' record_delimiter='\n' compression=auto);
" | $MYSQL_CLIENT_CONNECT

echo"
copy into region from @tpch_data files=('region.tbl') file_format=(type='CSV' field_delimiter='|' record_delimiter='\n' compression=auto);
" | $MYSQL_CLIENT_CONNECT

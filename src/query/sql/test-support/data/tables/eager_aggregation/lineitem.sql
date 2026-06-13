CREATE TABLE lineitem
(
    l_orderkey      BIGINT not null,
    l_partkey       BIGINT not null,
    l_suppkey       BIGINT not null,
    l_linenumber    BIGINT not null,
    l_quantity      DECIMAL(15, 2) not null,
    l_extendedprice DECIMAL(15, 2) not null,
    l_discount      DECIMAL(15, 2) not null,
    l_tax           DECIMAL(15, 2) not null,
    l_returnflag    STRING not null,
    l_linestatus    STRING not null,
    l_shipdate      DATE not null,
    l_commitdate    DATE not null,
    l_receiptdate   DATE not null,
    l_shipinstruct  STRING not null,
    l_shipmode      STRING not null,
    l_comment       STRING not null
)

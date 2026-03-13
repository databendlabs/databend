CREATE TABLE customer
(
    c_custkey     BIGINT not null,
    c_name        STRING not null,
    c_address     STRING not null,
    c_nationkey   INTEGER not null,
    c_phone       STRING not null,
    c_acctbal     DECIMAL(15, 2) not null,
    c_mktsegment  STRING not null,
    c_comment     STRING not null
)

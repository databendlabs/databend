-- Dynamic sqllogictest templates for the TPC-H concurrent CI workload.
-- The generator expands placeholders and writes .test files under generated/.
-- Keep the SQL here reviewable; randomness belongs in the generator inputs.

-- template: heavy_tpch_q21
statement ok
set sandbox_tenant = 'test_tenant';

statement ok
use tpch_test;

statement ok
set max_threads = {max_threads};

statement ok
SELECT
    s_name,
    count(*) AS numwait
FROM
    supplier,
    lineitem l1,
    orders,
    nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey)
    AND NOT EXISTS (
        SELECT
            *
        FROM
            lineitem l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate)
    AND s_nationkey = n_nationkey
    AND n_name = '{nation}'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT {limit};
-- endtemplate

-- template: explain_tpch_q9
statement ok
set sandbox_tenant = 'test_tenant';

statement ok
use tpch_test;

statement ok
EXPLAIN SELECT
    nation,
    o_year,
    truncate(truncate(sum(amount), 0) / {profit_divisor}, 0) AS sum_profit
FROM
    (
        SELECT
            n_name AS nation,
            extract(year FROM o_orderdate) AS o_year,
            truncate(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity, {truncate_scale}) AS amount
        FROM
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        WHERE
            s_suppkey = l_suppkey
            AND ps_suppkey = l_suppkey
            AND ps_partkey = l_partkey
            AND p_partkey = l_partkey
            AND o_orderkey = l_orderkey
            AND s_nationkey = n_nationkey
            AND p_name LIKE '%{part_token}%'
    ) AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    sum_profit
LIMIT {limit};
-- endtemplate

-- template: merge_into
statement ok
set sandbox_tenant = 'test_tenant';

statement ok
drop database if exists {database};

statement ok
create database {database};

statement ok
use {database};

statement ok
create table target_{case_id} (
    id int not null,
    flag int not null,
    amount decimal(18, 2) not null,
    payload string
) cluster by(id);

statement ok
create table source_{case_id} (
    id int not null,
    flag int not null,
    amount decimal(18, 2) not null,
    payload string
) cluster by(id);

statement ok
insert into target_{case_id} values {target_values};

statement ok
insert into source_{case_id} values {source_values};

query I
merge into target_{case_id}
using (
    select *
    from source_{case_id}
    where id % {source_mod} <> {source_skip}
) as src
on target_{case_id}.id = src.id
when matched and src.flag >= {flag_threshold} then
    update set target_{case_id}.amount = src.amount, target_{case_id}.payload = src.payload
----
{matched_count}

query I
merge into target_{case_id}
using (
    select *
    from source_{case_id}
    where id >= {insert_min_id}
) as src
on target_{case_id}.id = src.id
when not matched then
    insert (id, flag, amount, payload) values(src.id, src.flag, src.amount, src.payload)
----
{insert_count}

statement ok
drop database {database};
-- endtemplate

-- template: insert_replace
statement ok
set sandbox_tenant = 'test_tenant';

statement ok
drop database if exists {database};

statement ok
create database {database};

statement ok
use {database};

statement ok
create table fact_{case_id} (
    id bigint not null,
    customer_name string,
    order_priority string,
    amount decimal(18, 2)
) cluster by(id);

statement ok
create table high_{case_id} (id bigint not null, amount decimal(18, 2));

statement ok
create table low_{case_id} (id bigint not null, amount decimal(18, 2));

statement ok
create table replace_target_{case_id} (
    id bigint not null,
    amount decimal(18, 2),
    tag string
);

statement ok
insert into fact_{case_id}
select
    c_custkey + {id_offset},
    c_name,
    o_orderpriority,
    o_totalprice
from
    tpch_test.customer,
    tpch_test.orders
where
    c_custkey = o_custkey
    and o_orderdate >= to_date('{start_date}')
    and o_orderdate < add_months(to_date('{start_date}'), {months})
    and c_acctbal > {acctbal_threshold}
order by
    o_totalprice desc
limit {insert_limit};

statement ok
INSERT ALL
    WHEN amount >= {high_amount} THEN
        INTO high_{case_id} VALUES(id, amount)
    WHEN amount < {high_amount} THEN
        INTO low_{case_id} VALUES(id, amount)
SELECT id, amount FROM fact_{case_id};

statement ok
replace into replace_target_{case_id} on(id)
select
    id,
    amount,
    concat(order_priority, '_{tag_suffix}')
from (
    select
        id,
        amount,
        order_priority,
        row_number() over(partition by id order by amount desc) as rn
    from fact_{case_id}
    where amount >= {replace_min_amount}
) as dedup
where rn = 1;

statement ok
insert overwrite fact_{case_id}
select *
from fact_{case_id}
where amount >= {overwrite_min_amount};

statement ok
drop database {database};
-- endtemplate

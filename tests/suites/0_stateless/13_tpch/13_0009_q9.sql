
select
    nation,
    o_year,
    truncate(truncate(sum(amount),0)/10, 0) as sum_profit
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            truncate(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity, 100) as amount
        from
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        where
                s_suppkey = l_suppkey
          and ps_suppkey = l_suppkey
          and ps_partkey = l_partkey
          and p_partkey = l_partkey
          and o_orderkey = l_orderkey
          and s_nationkey = n_nationkey
          and p_name like '%green%'
    ) as profit
group by
    nation,
    o_year
order by
    sum_profit
limit 5;

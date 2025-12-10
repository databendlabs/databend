select o_orderpriority,
    count(*) as order_count
from orders
where o_orderdate >= to_date('1993-07-01')
    and o_orderdate < add_months(to_date('1993-07-01'), 3)
    and exists (
        select *
        from lineitem
        where l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
    )
group by o_orderpriority
order by o_orderpriority;

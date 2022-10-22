select
    c_custkey, count(o_orderkey) as c_count
from
    customer
        full outer join
    orders
    on c_custkey = o_custkey
        and o_comment not like '%pending%deposits%' and c_custkey > 100 and c_custkey < 120
group by
    c_custkey
order by c_custkey
    limit 20;
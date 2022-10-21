select
    c_custkey
from
    customer
        inner join
    orders
    on c_custkey = o_custkey
        and o_comment not like '%pending%deposits%' and c_custkey > 100 and c_custkey < 120 order by c_custkey limit 20;
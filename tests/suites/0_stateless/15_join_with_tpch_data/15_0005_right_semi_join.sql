set enable_cbo = 0;

select
    o_custkey
from
    customer
    right semi join
    orders
on c_custkey = o_custkey
    and o_comment not like '%pending%deposits%' and c_custkey > 100 and c_custkey < 120

order by o_custkey
    limit 20;

set enable_cbo = 1;
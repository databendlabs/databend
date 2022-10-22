set enable_cbo = 0;

select
    o_custkey
from
    customer
    right anti join
    orders
on c_custkey = o_custkey
    and o_comment not like '%pending%deposits%' and c_custkey > 100 and c_custkey < 120

order by o_custkey
    limit 20;

set enable_cbo = 1;

drop table customer;
drop table orders;
drop table lineitem;
drop table nation;
drop table region;
drop table supplier;
drop table part;
drop table partsupp;

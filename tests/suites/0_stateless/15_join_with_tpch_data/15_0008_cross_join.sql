select
    o_comment
from
    customer
    cross join
    orders
where o_comment not like '%pending%deposits%' and c_custkey > 100 and c_custkey < 120
order by o_comment
    limit 20;

drop table customer;
drop table orders;
drop table lineitem;
drop table nation;
drop table region;
drop table supplier;
drop table part;
drop table partsupp;
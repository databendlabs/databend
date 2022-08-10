select
    l_returnflag,
    l_linestatus,
    to_int64(sum(l_quantity)) as sum_qty,
    to_int64(sum(l_extendedprice)) as sum_base_price,
    to_int64(sum(l_extendedprice * (1 - l_discount))) as sum_disc_price,
    to_int64(sum(l_extendedprice * (1 - l_discount) * (1 + l_tax))) as sum_charge,
    to_int64(avg(l_quantity)) as avg_qty,
    to_int64(avg(l_extendedprice)) as avg_price,
    to_int64(avg(l_discount)) as avg_disc,
    count(*) as count_order
from
    lineitem
where
        l_shipdate <= add_days(to_date('1998-12-01'), 90)
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

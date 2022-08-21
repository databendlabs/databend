select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    to_int64(sum(l_extendedprice)) as sum_base_price,
    truncate(sum(l_extendedprice * (1 - l_discount)),2) as sum_disc_price,
    truncate(sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),2) as sum_charge,
    truncate(avg(l_quantity),4) as avg_qty,
    truncate(avg(l_extendedprice),4) as avg_price,
    truncate(avg(l_discount),4) as avg_disc,
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

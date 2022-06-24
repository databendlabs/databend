set enable_planner_v2 = 1;
select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= to_date('1994-01-01')
  and l_shipdate < addYears(to_date('1994-01-01'), 1)
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;
set enable_planner_v2 = 0;
set max_threads = 16;
explain select sum(number+1)+2 as sumx from numbers_mt(80000) where (number+1)=4 limit 1;

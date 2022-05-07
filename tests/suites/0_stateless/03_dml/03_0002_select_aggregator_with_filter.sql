set enable_planner_v2 = 1;
SELECT max(number) FROM numbers_mt (10) where number > 99999999998;
SELECT max(number) FROM numbers_mt (10) where number > 2;
set enable_planner_v2 = 0;
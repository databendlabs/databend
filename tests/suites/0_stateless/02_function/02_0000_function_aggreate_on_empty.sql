-- return default
select min(number) from numbers_mt (10) where 1 = 2;
select max(number) from numbers_mt (10) where 1 = 2;
select arg_min(number, number) from numbers_mt (10) where 1 = 2;
select arg_max(number, number) from numbers_mt (10) where 1 = 2;
select sum_distinct(number) from numbers(10) where 1 = 2;
select sum_if(number, number > 100) from numbers(10);
select sum_if(number, number > 0) from numbers(10) where 1 = 2;

-- return zero
select count(number) from numbers_mt (10) where 1 = 2;
select uniq(number) from numbers_mt (10) where 1 = 2;
select count_distinct(number) from numbers (10) where 1 = 2;
select count(distinct number) from numbers (10) where 1 = 2;
select count_if(number, number > 100) from numbers (10);
select count_if(number, number > 0) from numbers (10) where 1 = 2;

-- return empty or nan
select min(number) from numbers_mt (10) where 1 = 2 group by number % 2;
select max(number) from numbers_mt (10) where 1 = 2 group by number % 2;
select arg_min(number, number) from numbers_mt (10) where 1 = 2 group by number % 2;
select arg_max(number, number) from numbers_mt (10) where 1 = 2 group by number % 2;
select count(number) from numbers_mt (10) where 1 = 2 group by number % 2;
select uniq(number) from numbers_mt (10) where 1 = 2 group by number % 2;
select count_distinct(number) from numbers (10) where 1 = 2 group by number % 2;
select count(distinct number) from numbers (10) where 1 = 2 group by number % 2;
select count_if(number, number > 100) from numbers (10) group by number % 2;
select count_if(number, number > 0) from numbers (10) where 1 = 2 group by number % 2;

-- constant related
select min(1) from numbers_mt (10) where 1=2;
select max(1) from numbers_mt (10) where 1=2;
select arg_min(number, 1) from numbers_mt (10) where 1=2;
select arg_min(1, number) from numbers_mt (10) where 1=2;
select arg_min(1, 1) from numbers_mt (10) where 1=2;
select arg_max(number, 1) from numbers_mt (10) where 1=2;
select arg_max(1, number) from numbers_mt (10) where 1=2;
select arg_max(1, 1) from numbers_mt (10) where 1=2;

-- nan
select avg(number) from numbers_mt (10) where 1 = 2;
select avg(1) from numbers_mt (10) where 1=2;
select avg(number) from numbers_mt (10) where 1 = 2 group by number % 2;


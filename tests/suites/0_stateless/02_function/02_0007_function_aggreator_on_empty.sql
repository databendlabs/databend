-- return default
select min(number) from numbers_mt (10) where 1 = 2;
select max(number) from numbers_mt (10) where 1 = 2;
select argmin(number, number) from numbers_mt (10) where 1 = 2;
select argmax(number, number) from numbers_mt (10) where 1 = 2;
select sumDistinct(number) from numbers(10) where 1 = 2;
select sumIf(number, number > 100) from numbers(10);
select sumIf(number, number > 0) from numbers(10) where 1 = 2;


-- return zero
select count(number) from numbers_mt (10) where 1 = 2;
select uniq(number) from numbers_mt (10) where 1 = 2;
select countDistinct(number) from numbers (10) where 1 = 2;
select count(distinct number) from numbers (10) where 1 = 2;
select countIf(number, number > 100) from numbers (10);
select countIf(number, number > 0) from numbers (10) where 1 = 2;

-- return empty or nan
select min(number) from numbers_mt (10) where 1 = 2 group by number % 2;
select max(number) from numbers_mt (10) where 1 = 2 group by number % 2;
select argmin(number, number) from numbers_mt (10) where 1 = 2 group by number % 2;
select argmax(number, number) from numbers_mt (10) where 1 = 2 group by number % 2;
select count(number) from numbers_mt (10) where 1 = 2 group by number % 2;
select uniq(number) from numbers_mt (10) where 1 = 2 group by number % 2;
select countDistinct(number) from numbers (10) where 1 = 2 group by number % 2;
select count(distinct number) from numbers (10) where 1 = 2 group by number % 2;
select countIf(number, number > 100) from numbers (10) group by number % 2;
select countIf(number, number > 0) from numbers (10) where 1 = 2 group by number % 2;

-- constant related
select min(1) from numbers_mt (10) where 1=2;
select max(1) from numbers_mt (10) where 1=2;
select argmin(number, 1) from numbers_mt (10) where 1=2;
select argmin(1, number) from numbers_mt (10) where 1=2;
select argmin(1, 1) from numbers_mt (10) where 1=2;
select argmax(number, 1) from numbers_mt (10) where 1=2;
select argmax(1, number) from numbers_mt (10) where 1=2;
select argmax(1, 1) from numbers_mt (10) where 1=2;


-- nan
select avg(number) from numbers_mt (10) where 1 = 2;
select avg(1) from numbers_mt (10) where 1=2;
select avg(number) from numbers_mt (10) where 1 = 2 group by number % 2;


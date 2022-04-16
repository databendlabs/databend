-- distinct
select sum_distinct(number) from ( select number % 100 as number from numbers(100000));
select count_distinct(number) from ( select number % 100 as number from numbers(100000));
select sum_distinct(number) /  count_distinct(number) = avg_distinct(number) from ( select number % 100 as number from numbers(100000));

-- if
select sumIf(number, number >= 100000 - 1) from numbers(100000);
select sumIf(number, number > 100) /  countIf(number,  number > 100) = avgIf(number,  number > 100) from numbers(100000);
select countIf(number, number>9) from numbers(10);

-- boolean
select sum(number > 314) from numbers(1000);
select avg(number > 314) from numbers(1000);

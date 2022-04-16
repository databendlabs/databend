-- distinct
select sum_distinct(number) from ( select number % 100 as number from numbers(100000));
select count_distinct(number) from ( select number % 100 as number from numbers(100000));
select sum_distinct(number) /  count_distinct(number) = avg_distinct(number) from ( select number % 100 as number from numbers(100000));

-- if
select sum_if(number, number >= 100000 - 1) from numbers(100000);
select sum_if(number, number > 100) /  count_if(number,  number > 100) = avg_if(number,  number > 100) from numbers(100000);
select count_if(number, number>9) from numbers(10);

-- boolean
select sum(number > 314) from numbers(1000);
select avg(number > 314) from numbers(1000);

-- distinct
select sumDistinct(number) from ( select number % 100 as number from numbers(100000));
select countDistinct(number) from ( select number % 100 as number from numbers(100000));
select sumDistinct(number) /  countDistinct(number) = avgDistinct(number) from ( select number % 100 as number from numbers(100000));

-- if
select sumIf(number, number >= 100000 - 1) from numbers(100000);
select sumIf(number, number > 100) /  countIf(number,  number > 100) = avgIf(number,  number > 100) from numbers(100000);
select countIf(number, number>9) from numbers(10);

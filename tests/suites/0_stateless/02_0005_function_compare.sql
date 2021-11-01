-- https://github.com/datafuselabs/databend/issues/492
SELECT number ,number-1 , number*100 , 1> 100 ,1 < 10 FROM numbers_mt (10) order by number;

-- between
select number from numbers_mt(10) where number  not between 4 + 0.1  and 8 - 0.1  order by number;
select number from numbers_mt(10) where number   between 4 + 0.1  and 8 - 0.1  order by number;

-- like
select * from system.databases where name like '%sys%';
select * from system.databases where name like '_ef_ul_';

-- not like
select * from system.databases where name not like '%sys%' order by name;
select * from system.databases where name not like '_ef_ul_' order by name;

select * from numbers(10) where null = true;
select * from numbers(10) where null and true;


select '==compare_number_string==';
-- todo: partial parse case
-- select '123 ab' = 123;
select '123' = 123;
select '7.4' = 7.4;
select '7.4' > 7;
select '777.4' < 778;

select '==compare_datetime==';
-- compare with date/datetime strings
SELECT '2021-03-05' = toDate('2021-03-05');
SELECT '2021-03-05 01:01:01' = toDateTime('2021-03-05 01:01:01');
SELECT '2021-03-05 01:01:02' > toDateTime('2021-03-05 01:01:01');
SELECT '2021-03-06' > toDate('2021-03-05');
SELECT toDateTime('2021-03-05 00:00:00') = toDate('2021-03-05');
SELECT toDateTime('2021-03-05 00:00:01') > toDate('2021-03-05');
SELECT toDateTime('2021-03-04 00:00:01') < toDate('2021-03-05');
SELECT toDateTime(toDate('2021-03-05')) = toDate('2021-03-05');

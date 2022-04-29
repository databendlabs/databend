SELECT sum(number) from numbers_mt(10000);
SELECT min(number) from numbers_mt(10000);
SELECT max(number) from numbers_mt(10000);
SELECT avg(number) from numbers_mt(10000);
SELECT count(number) from numbers_mt(10000);
SELECT sum(number)/count(number) from numbers_mt(10000);
SELECT arg_min(number, number) from numbers_mt(10000);
SELECT arg_min(a, b) from (select number + 5 as a, number - 5 as b from numbers_mt(10000));
SELECT arg_min(b, a) from (select number + 5 as a, number - 5 as b from numbers_mt(10000));
SELECT arg_max(number, number) from numbers_mt(10000);
SELECT arg_max(a, b) from (select number + 5 as a, number - 5 as b from numbers_mt(10000));
SELECT arg_max(b, a) from (select number + 5 as a, number - 5 as b from numbers_mt(10000));

-- test arg_max, arg_min fro String
SELECT arg_max(a, b) from (select number + 5 as a, cast(number as varchar(255)) as b from numbers_mt(10000)) ;
SELECT arg_max(b, a) from (select number + 5 as a, cast(number as varchar(255)) as b from numbers_mt(10000)) ;


select count(distinct number, number + 1 , number + 3 ) from ( select number % 100 as number from numbers(100000));
select count(distinct 3) from numbers(10000);
select uniq(number, number + 1 , number + 3 )  =  count(distinct number, number + 1 , number + 3 ) from ( select number % 100 as number from numbers(100000));
SELECT std(number) between  2886.751 and 2886.752 from numbers_mt(10000);
SELECT stddev(number) between  2886.751 and 2886.752 from numbers_mt(10000);
SELECT stddev_pop(number) between  2886.751 and 2886.752 from numbers_mt(10000);
SELECT covar_samp(number, number) from (select * from numbers_mt(5) order by number asc);
SELECT covar_pop(number, number) from (select * from numbers_mt(5) order by number asc);
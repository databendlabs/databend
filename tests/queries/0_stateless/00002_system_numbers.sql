select number + 2, number * 2, number / 2 from system.numbers(3);
select max(a) + max(b) from (SELECT number + 10 as a, number + 4 as b from system.numbers(10));

SELECT sum(number) from system.numbers_mt(10000);
SELECT min(number) from system.numbers_mt(10000);
SELECT max(number) from system.numbers_mt(10000);
SELECT avg(number) from system.numbers_mt(10000);
SELECT count(number) from system.numbers_mt(10000);
SELECT sum(number)/count(number) from system.numbers_mt(10000);


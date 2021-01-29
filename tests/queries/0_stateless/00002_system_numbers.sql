SELECT sum(number) from system.numbers_mt(10000);
SELECT min(number) from system.numbers_mt(10000);
SELECT max(number) from system.numbers_mt(10000);
SELECT avg(number) from system.numbers_mt(10000);
SELECT count(number) from system.numbers_mt(10000);
SELECT sum(number)/count(number) from system.numbers_mt(10000);

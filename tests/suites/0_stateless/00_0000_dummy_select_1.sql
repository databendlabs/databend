SELECT 1;
SELECT x; -- {ErrorCode 1002}
SELECT 'a';
SELECT NOT(1=1);
SELECT NOT(1);
SELECT NOT(1=1) from numbers(3);
SELECT TRUE;
SELECT FALSE;
SELECT NOT(TRUE);
select number from numbers_mt(10) where number > 5  and exists (select name from system.settings);
select number from numbers_mt(10) where number > 5  and exists (select name from system.settings) and exists (select number from numbers_mt(10));
select number from numbers_mt(10) where number > 5  and exists (select name from system.settings where exists (select number from numbers_mt(10)));
select number from numbers_mt(20) where number > 15  and not exists (select number from numbers_mt(5) where number > 10);
SELECT a.number FROM numbers(3) AS a order by a.number;
SELECT a.number FROM (SELECT * FROM numbers(3) AS b ORDER BY b.number) AS a;
SELECT b.number FROM numbers(3) AS a ORDER BY a.number; -- {ErrorCode 25}
SELECT a.number FROM numbers(3) AS a ORDER BY b.number; -- {ErrorCode 25}
SELECT b.number FROM (SELECT * FROM numbers(3) AS b ORDER BY a.number) AS a; -- {ErrorCode 25}
SELECT b.number FROM (SELECT * FROM numbers(3) AS b) AS a ORDER BY b.number; -- {ErrorCode 25}

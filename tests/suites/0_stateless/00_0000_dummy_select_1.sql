SELECT 1;
SELECT x;
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
SELECT a.number FROM numbers(3) AS a order by a.number;
SELECT a.number FROM (SELECT * FROM numbers(3) AS b ORDER BY b.number) AS a;
SELECT b.number FROM numbers(3) AS a ORDER BY a.number;
SELECT a.number FROM numbers(3) AS a ORDER BY b.number;
SELECT b.number FROM (SELECT * FROM numbers(3) AS b ORDER BY a.number) AS a;
SELECT b.number FROM (SELECT * FROM numbers(3) AS b) AS a ORDER BY b.number;


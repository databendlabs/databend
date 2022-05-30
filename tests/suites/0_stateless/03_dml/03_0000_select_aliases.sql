select 1 as a, 1 + 1 as b, 1 + 1 + 1 as c;
select 3 x;
select 1 - number  as number from numbers(3) order by number;
select number * number +  1 as number, number + 1 as b from numbers(1);
select number * number +  1 as `number`, number + 1 as `b` from numbers(1);
select number * number +  1 `number`, number + 1 `b` from numbers(1);

SELECT number, 'number' FROM numbers(3) AS a order by number;
SELECT a.number FROM numbers(3) AS a order by number;
SELECT a.number FROM (SELECT * FROM numbers(3) AS b ORDER BY number) AS a;
-- SELECT b.number FROM numbers(3) AS a ORDER BY a.number; -- {ErrorCode 1058}
-- SELECT a.number FROM numbers(3) AS a ORDER BY b.number; -- {ErrorCode 1058}
-- SELECT b.number FROM (SELECT * FROM numbers(3) AS b ORDER BY a.number) AS a; -- {ErrorCode 1058}
-- SELECT b.number FROM (SELECT * FROM numbers(3) AS b) AS a ORDER BY b.number; -- {ErrorCode 1058}

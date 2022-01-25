SELECT 1;
SELECT x; -- {ErrorCode 1058}
SELECT 'a';
SELECT NOT(1=1);
SELECT NOT(1);
SELECT NOT(1=1) from numbers(3);
SELECT TRUE;
SELECT FALSE;
SELECT NOT(TRUE);
SELECT a.number FROM numbers(3) AS a order by a.number;
SELECT a.number FROM (SELECT * FROM numbers(3) AS b ORDER BY b.number) AS a;
SELECT b.number FROM numbers(3) AS a ORDER BY a.number; -- {ErrorCode 1058}
SELECT a.number FROM numbers(3) AS a ORDER BY b.number; -- {ErrorCode 1058}
SELECT b.number FROM (SELECT * FROM numbers(3) AS b ORDER BY a.number) AS a; -- {ErrorCode 1058}
SELECT b.number FROM (SELECT * FROM numbers(3) AS b) AS a ORDER BY b.number; -- {ErrorCode 1058}
SELECT number, 'number', "number" FROM numbers(3) AS a order by a.number;
SELECT 'That\'s good.';

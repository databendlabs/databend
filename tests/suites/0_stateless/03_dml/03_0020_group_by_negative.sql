SELECT (0 - number) :: Int8 AS c FROM numbers(256) GROUP BY c HAVING c = 0;
SELECT (0 - number) :: Int8 AS c FROM numbers(256) GROUP BY c HAVING c = -1;
SELECT (0 - number) :: Int8 AS c FROM numbers(256) GROUP BY c HAVING c = -127;
SELECT (0 - number) :: Int8 AS c FROM numbers(256) GROUP BY c HAVING c = -128;
SELECT COUNT() FROM (SELECT (0 - number) :: Int8 AS c FROM numbers(256) GROUP BY c);
SELECT COUNT() FROM (SELECT (0 - number) :: Int8 AS c FROM numbers(256) GROUP BY c HAVING c > 0);

SELECT (0 - number) :: Int16 AS c FROM numbers(65535) GROUP BY c HAVING c = 0;
SELECT (0 - number) :: Int16 AS c FROM numbers(65535) GROUP BY c HAVING c = -1;
SELECT (0 - number) :: Int16 AS c FROM numbers(65535) GROUP BY c HAVING c = -32768;
SELECT (0 - number) :: Int16 AS c FROM numbers(65535) GROUP BY c HAVING c = -32769;
SELECT COUNT() FROM (SELECT (0 - number) :: Int16 AS c FROM numbers(65535) GROUP BY c);
SELECT COUNT() FROM (SELECT (0 - number) :: Int16 AS c FROM numbers(65535) GROUP BY c HAVING c > 0);

SELECT (0 - number) :: Int32 AS c FROM numbers(65535) GROUP BY c HAVING c = 0;
SELECT (0 - number) :: Int32 AS c FROM numbers(65535) GROUP BY c HAVING c = -1;
SELECT (0 - number) :: Int32 AS c FROM numbers(65535) GROUP BY c HAVING c = -32768;
SELECT (0 - number) :: Int32 AS c FROM numbers(65535) GROUP BY c HAVING c = -32769;
SELECT COUNT() FROM (SELECT (0 - number) :: Int32 AS c FROM numbers(65535) GROUP BY c);
SELECT COUNT() FROM (SELECT (0 - number) :: Int32 AS c FROM numbers(65535) GROUP BY c HAVING c > 0);

SELECT (0 - number) :: Int64 AS c FROM numbers(65535) GROUP BY c HAVING c = 0;
SELECT (0 - number) :: Int64 AS c FROM numbers(65535) GROUP BY c HAVING c = -1;
SELECT (0 - number) :: Int64 AS c FROM numbers(65535) GROUP BY c HAVING c = -32768;
SELECT (0 - number) :: Int64 AS c FROM numbers(65535) GROUP BY c HAVING c = -32769;
SELECT COUNT() FROM (SELECT (0 - number) :: Int64 AS c FROM numbers(65535) GROUP BY c);
SELECT COUNT() FROM (SELECT (0 - number) :: Int64 AS c FROM numbers(65535) GROUP BY c HAVING c > 0);

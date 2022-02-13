SELECT '=== LEFT null ===';
SELECT LEFT(NULL, 1);
SELECT LEFT('aaa', NULL);
SELECT LEFT(NULL, NULL);

SELECT '=== LEFT const, const ===';
SELECT LEFT('', 0);
SELECT LEFT('', 1);
SELECT LEFT('123', 0);
SELECT LEFT('123', 1);
SELECT LEFT('123', 2);
SELECT LEFT('123', 3);
SELECT LEFT('123', 4);

SELECT '=== LEFT const, series ===';
SELECT LEFT('123', number) FROM numbers(5) ORDER BY number;

SELECT '=== LEFT series, const ===';
SELECT LEFT(toString(number * 10000), 1) FROM numbers(5) ORDER BY number;

SELECT '=== LEFT series, series ===';
SELECT LEFT(toString(number * 10000), number) FROM numbers(5) ORDER BY number;

SELECT '=== RIGHT null ===';
SELECT RIGHT(NULL, 1);
SELECT RIGHT('aaa', NULL);
SELECT RIGHT(NULL, NULL);

SELECT '=== RIGHT const, const ===';
SELECT RIGHT('', 0);
SELECT RIGHT('', 1);
SELECT RIGHT('123', 0);
SELECT RIGHT('123', 1);
SELECT RIGHT('123', 2);
SELECT RIGHT('123', 3);
SELECT RIGHT('123', 4);

SELECT '=== RIGHT const, series ===';
SELECT RIGHT('123', number) FROM numbers(5) ORDER BY number;

SELECT '=== RIGHT series, const ===';
SELECT RIGHT(toString(number * 10000), 1) FROM numbers(5) ORDER BY number;

SELECT '=== RIGHT series, series ===';
SELECT RIGHT(toString(number * 10000), number) FROM numbers(5) ORDER BY number;

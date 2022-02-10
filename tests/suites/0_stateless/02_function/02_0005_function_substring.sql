SELECT '=== null ===';
SELECT SUBSTRING('12345', null);
SELECT SUBSTRING(null, 2);
SELECT SUBSTRING(null, null);

SELECT '=== const, const ===';
SELECT SUBSTRING('12345', 0);
SELECT SUBSTRING('12345', 1);
SELECT SUBSTRING('12345', 5);
SELECT SUBSTRING('12345', 6);
SELECT '=== const, const, const ===';
SELECT SUBSTRING('12345', 2, 0);
SELECT SUBSTRING('12345', 2, 1);
SELECT SUBSTRING('12345', 2, 5);
SELECT SUBSTRING('12345', 2, 6);
SELECT '=== const, const, series ===';
SELECT SUBSTRING('12345', 2, number) FROM numbers(7) ORDER BY number;

SELECT '=== const, series ===';
SELECT SUBSTRING('12345', number) FROM numbers(7) ORDER BY number;
SELECT '=== const, series, const ===';
SELECT SUBSTRING('12345', number, 2) FROM numbers(7) ORDER BY number;
SELECT '=== const, series, series ===';
SELECT SUBSTRING('12345', number, number) FROM numbers(7) ORDER BY number;

SELECT '=== series, const ===';
SELECT SUBSTRING(toString(number * 100 + number), 2) FROM numbers(7) ORDER BY number;
SELECT '=== series, const, const ===';
SELECT SUBSTRING(toString(number * 100 + number), 1, 1) FROM numbers(7) ORDER BY number;
SELECT '=== series, const, series ===';
SELECT SUBSTRING(toString(number * 100 + number), 1, number) FROM numbers(7) ORDER BY number;

SELECT '=== series, series ===';
SELECT SUBSTRING(toString(number * 100 + number), number) FROM numbers(7) ORDER BY number;
SELECT '=== series, series, const ===';
SELECT SUBSTRING(toString(number * 100 + number), number, 1) FROM numbers(7) ORDER BY number;
SELECT '=== series, series, series ===';
SELECT SUBSTRING(toString(number * 100 + number), number, number) FROM numbers(7) ORDER BY number;

SELECT '=== forms ===';
SELECT SUBSTRING('12345' FROM 2);
SELECT SUBSTRING('12345' FROM 2 FOR 1);

SELECT '=== synonyms ===';
SELECT MID('12345', 2, 1);
SELECT SUBSTR('12345', 2, 1);

SELECT '=== null ===';
SELECT REPLACE(NULL, 'a', 'b');
SELECT REPLACE('aaa', 'a', NULL);
SELECT REPLACE('aaa', NULL, NULL);
SELECT REPLACE(NULL, NULL, NULL);

SELECT '=== const, const, const ===';
SELECT REPLACE('aaaa123aaa456aa7a', 'a', 'b');
SELECT REPLACE('aaaa123aaa456aa7a', '', 'b');
SELECT REPLACE('aaaa123aaa456aa7a', 'a', '');

SELECT '=== const, const, series ===';
SELECT REPLACE('a1b', '1', toString(number)) FROM numbers(5) ORDER BY number;

SELECT '=== const, series, const ===';
SELECT REPLACE('a1b', toString(number), '7') FROM numbers(5) ORDER BY number;

SELECT '=== const, series, series ===';
SELECT REPLACE('a1b', toString(number), toString(number + 1)) FROM numbers(5) ORDER BY number;

SELECT '=== series, const, const ===';
SELECT REPLACE(toString(number * 10), '0', '1') FROM numbers(5) ORDER BY number;

SELECT '=== series, const, series ===';
SELECT REPLACE(toString(number * 10), '0', toString(number)) FROM numbers(5) ORDER BY number;

SELECT '=== series, series, const ===';
SELECT REPLACE(toString(number * 10), toString(number), '1') FROM numbers(5) ORDER BY number;

SELECT '=== series, series, series ===';
SELECT REPLACE(toString(number * 10), toString(number), toString(number)) FROM numbers(5) ORDER BY number;
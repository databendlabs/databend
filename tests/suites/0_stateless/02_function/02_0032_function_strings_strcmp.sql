SELECT '=== null ===';
SELECT STRCMP(NULL, 'a');
SELECT STRCMP('a', NULL);
SELECT STRCMP(NULL, NULL);

SELECT '=== const, const ===';
SELECT STRCMP('', '123');
SELECT STRCMP('123', '');
SELECT STRCMP('123', '123');
SELECT STRCMP('1234', '123');
SELECT STRCMP('123', '1234');
SELECT STRCMP('123', '153');

SELECT '=== const, series ===';
SELECT STRCMP('2', toString(number)) FROM numbers(5) ORDER BY number;

SELECT '=== series, const ===';
SELECT STRCMP(toString(number), '3') FROM numbers(5) ORDER BY number;

SELECT '=== series, series ===';
SELECT STRCMP(toString(number), toString(number)) FROM numbers(5) ORDER BY number;
SELECT STRCMP(toString(number + 1), toString(number)) FROM numbers(5) ORDER BY number;
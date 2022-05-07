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
SELECT STRCMP('2', to_varchar(number)) FROM numbers(5) ORDER BY number;

SELECT '=== series, const ===';
SELECT STRCMP(to_varchar(number), '3') FROM numbers(5) ORDER BY number;

SELECT '=== series, series ===';
SELECT STRCMP(to_varchar(number), to_varchar(number)) FROM numbers(5) ORDER BY number;
SELECT STRCMP(to_varchar(number + 1), to_varchar(number)) FROM numbers(5) ORDER BY number;
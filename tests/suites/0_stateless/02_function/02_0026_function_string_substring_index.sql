SELECT '=== null ===';
SELECT SUBSTRING_INDEX(NULL, '.', 1);
SELECT SUBSTRING_INDEX('www', NULL, 1);
SELECT SUBSTRING_INDEX('www', '.', NULL);
SELECT SUBSTRING_INDEX('www', NULL, NULL);
SELECT SUBSTRING_INDEX(NULL, NULL, NULL);

SELECT '=== const, const, const ===';
SELECT SUBSTRING_INDEX('www__databend__shauneu__mike', '__', 1);
SELECT SUBSTRING_INDEX('www__databend__shauneu__mike', '__', 2);
SELECT SUBSTRING_INDEX('www__databend__shauneu__mike', '__', 3);
SELECT SUBSTRING_INDEX('www__databend__shauneu__mike', '__', 4);
SELECT SUBSTRING_INDEX('www__databend__shauneu__mike', '__', -1);
SELECT SUBSTRING_INDEX('www__databend__shauneu__mike', '__', -2);
SELECT SUBSTRING_INDEX('www__databend__shauneu__mike', '__', -3);
SELECT SUBSTRING_INDEX('www__databend__shauneu__mike', '__', -4);

SELECT '=== const, const, series ===';
SELECT SUBSTRING_INDEX('www__databend__shauneu__mike', '__', number) FROM numbers(5) ORDER BY number;

SELECT '=== const, series, const ===';
SELECT SUBSTRING_INDEX('www_1_databend_2_shauneu_3_mike', number, 1) FROM numbers(5) ORDER BY number;

SELECT '=== const, series, series ===';
SELECT SUBSTRING_INDEX('www_1_databend_2_shauneu_3_mike', number, number) FROM numbers(5) ORDER BY number;

SELECT '=== series, const, const ===';
SELECT SUBSTRING_INDEX(number + 10, '0', 1) FROM numbers(5) ORDER BY number;

SELECT '=== series, const, series ===';
SELECT SUBSTRING_INDEX(number + 10, '0', number) FROM numbers(5) ORDER BY number;

SELECT '=== series, series, const ===';
SELECT SUBSTRING_INDEX(number + 10, number, 1) FROM numbers(5) ORDER BY number;

SELECT '=== series, series, series ===';
SELECT SUBSTRING_INDEX(number + 10, number, number) FROM numbers(5) ORDER BY number;


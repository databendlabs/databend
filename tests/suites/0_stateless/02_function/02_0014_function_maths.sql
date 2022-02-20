CREATE TABLE math_sample_numbers (timestamp UInt32, value Int32) engine=Memory;
INSERT INTO math_sample_numbers VALUES ('1', '-1'), ('2', '-2'), ('3', '3');

SELECT pi();

SELECT '===abs===';

SELECT abs(-1);
SELECT abs(-10086);
SELECT abs('-233.0'); -- {ErrorCode 1007}
SELECT abs('blah'); -- {ErrorCode 1007}
SELECT abs(TRUE); -- {ErrorCode 1007}
select abs(-9223372036854775808); -- {ErrorCode 1049}
SELECT abs(NULL);
SELECT abs(value) FROM math_sample_numbers;
SELECT abs(value) + abs(-1) FROM math_sample_numbers;

SELECT '===log===';
CREATE TABLE math_log_numbers (a Float, b Float) engine=Memory;
INSERT INTO math_log_numbers VALUES (2.0, 1024.0), (NULL, 12), (12, NULL);

SELECT log(NULL);
SELECT log(NULL, NULL);
SELECT log(1, NULL);
SELECT log(NULL, 1);
SELECT log(10, 100);
SELECT ln(NULL);
SELECT ln(1, 2);
SELECT log10(NULL);
SELECT log10(100);
SELECT log2(2);
SELECT log(a, b) FROM math_log_numbers;

DROP TABLE math_log_numbers;

SELECT '===mod===';

SELECT mod(234, 10);
SELECT mod(29, 9);
SELECT mod(34.5, 3);

SELECT '===exp===';

SELECT exp(NULL);
SELECT exp(2);
SELECT exp('2'); -- {ErrorCode 1007}

SELECT '===trigonometric===';

SELECT sin(0);
SELECT cos(0);
SELECT tan(0);
SELECT tan(pi()/4);
SELECT cot(0);
SELECT cot(pi()/4);
SELECT asin(0.2);
SELECT asin(1.1);
SELECT acos(1);
SELECT acos(1.0001);
SELECT atan(1);
SELECT atan(-1);
SELECT atan(-2, 2);
SELECT atan2(-2, 2);
SELECT atan2(pi(), 0);
SELECT atan2(-2, NULL);
SELECT atan2(NULL, 2);
SELECT atan2(NULL, NULL);
SELECT atan2(NULL, number) from numbers(2);
SELECT atan2(number, NULL) from numbers(2);

SELECT '===sqrt===';

SELECT sqrt(4);
SELECT sqrt(0);
SELECT sqrt(-4);
SELECT sqrt('a'); -- {ErrorCode 1007}

SELECT '===pow===';

SELECT pow(2, 2);
SELECT pow(-2, 2);
SELECT pow(2, -2);
SELECT pow(NULL, 2);
SELECT pow(2, NULL);
SELECT pow(NULL, number) from numbers(2);
SELECT pow(number, NULL) from numbers(2);
SELECT pow('a', 2); -- {ErrorCode 1007}
SELECT pow(2, 'a'); -- {ErrorCode 1007}

DROP TABLE math_sample_numbers;

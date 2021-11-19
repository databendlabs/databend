CREATE TABLE math_sample_numbers (timestamp UInt32, value Int32) engine=Memory;
INSERT INTO math_sample_numbers VALUES ('1', '-1'), ('2', '-2'), ('3', '3');

SELECT pi();
SELECT abs(-1);
SELECT abs(-10086);
SELECT abs('-233.0');
SELECT abs('blah') = 0;
SELECT abs(TRUE); -- {ErrorCode 7}
SELECT abs(NULL); -- {ErrorCode 7}
SELECT abs(value) FROM math_sample_numbers;
SELECT abs(value) + abs(-1) FROM math_sample_numbers;
-- TODO: log(NULL) should returns NULL
SELECT log(NULL);
SELECT log(NULL, NULL); -- {ErrorCode 10}
SELECT log(1, NULL);
SELECT log(NULL, 1); -- {ErrorCode 10}
SELECT log('10', 100);
SELECT ln(NULL);
SELECT ln(1, 2); -- {ErrorCode 28}
SELECT log10(NULL);
SELECT log10(100);
SELECT log2(2);
SELECT mod(234, 10);
SELECT mod(29, 9);
SELECT mod(34.5, 3);
SELECT exp(NULL);
SELECT exp(2);
SELECT exp('2');
SELECT exp('a');
SELECT sin(0);
SELECT sin('0');
SELECT sin('foo');
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
SELECT sqrt(4);
SELECT sqrt(0);
SELECT sqrt(-4);
SELECT sqrt('a');

DROP TABLE math_sample_numbers;

CREATE TABLE math_sample_numbers (timestamp UInt32, value Int32) engine=Memory;
INSERT INTO math_sample_numbers VALUES ('1', '-1'), ('2', '-2'), ('3', '3');

SELECT pi();
SELECT abs(-1);
SELECT abs(-10086);
SELECT abs('-233.0');
SELECT abs('blah');
SELECT abs(TRUE); -- {ErrorCode 7}
SELECT abs(NULL); -- {ErrorCode 7}
SELECT abs(value) FROM math_sample_numbers;
SELECT abs(value) + abs(-1) FROM math_sample_numbers;

DROP TABLE math_sample_numbers;

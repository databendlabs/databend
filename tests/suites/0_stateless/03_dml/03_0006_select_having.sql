-- SELECT * FROM numbers_mt (3) having number > 5;
-- SELECT * FROM numbers_mt (3) having number > 1;
-- SELECT * FROM numbers_mt (3) having 1=2;
SELECT MAX(number) AS max FROM numbers_mt(10) GROUP BY number%3 HAVING MAX(number)>8;
SELECT MAX(number) AS max FROM numbers_mt(10) GROUP BY number%3 HAVING MAX(number)>7 ORDER BY max;
SELECT MAX(number) AS max FROM numbers_mt(10) GROUP BY number%3 HAVING MAX(number)<7;

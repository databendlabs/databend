CREATE TABLE t(a UInt64, b UInt32) Engine = Fuse;
INSERT INTO t(a,b)  SELECT if (number % 3 = 1, null, number) as a, number + 3 as b FROM numbers(10);
SELECT a%3 as c, count(1) as d from t GROUP BY c ORDER BY c, d;
SELECT a%3 as c, a%4 as d, count(0) as f FROM t GROUP BY c,d ORDER BY c,d,f;
DROP table t;
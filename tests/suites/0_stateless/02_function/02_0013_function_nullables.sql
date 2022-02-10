DROP TABLE IF EXISTS nullable_test;

CREATE TABLE nullable_test (a UInt32, b UInt32) engine=Memory;
INSERT INTO nullable_test VALUES(1, Null), (Null, 2), (3, 3);

SELECT a, isNull(a), b, isNotNull(b) FROM nullable_test ORDER BY a, b ASC;
SELECT a FROM nullable_test WHERE a is Not Null ORDER BY a;
SELECT b FROM nullable_test WHERE a is Null ORDER BY b;

DROP TABLE IF EXISTS nullable_test;

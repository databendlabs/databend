DROP TABLE IF EXISTS nullable_test;

CREATE TABLE nullable_test (a UInt32 null, b UInt32 null) engine=Memory;
INSERT INTO nullable_test VALUES(1, Null), (Null, 2), (3, 3);

SELECT a, is_null(a), b, is_not_null(b) FROM nullable_test ORDER BY a, b ASC;
SELECT a FROM nullable_test WHERE a is Not Null ORDER BY a;
SELECT b FROM nullable_test WHERE a is Null ORDER BY b;

DROP TABLE IF EXISTS nullable_test;

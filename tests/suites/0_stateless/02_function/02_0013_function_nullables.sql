DROP TABLE IF EXISTS nullable_test;

CREATE TABLE nullable_test (a UInt32 null, b UInt32 null, c UInt32) Engine = Fuse;
INSERT INTO nullable_test VALUES(1, Null, 1), (Null, 2, 2), (3, 3, 3);

SELECT a, is_null(a), b, is_not_null(b) FROM nullable_test ORDER BY c ASC;
SELECT a FROM nullable_test WHERE a is Not Null ORDER BY c;
SELECT b FROM nullable_test WHERE a is Null ORDER BY c;


SELECT assume_not_null(null); -- {ErrorCode 1007}
SELECT assume_not_null(a), assume_not_null(b), assume_not_null(c) from nullable_test ORDER BY c;

SELECT to_nullable(3), to_nullable(null);

DROP TABLE IF EXISTS nullable_test;

SET enable_planner_v2 = 1;

SELECT '==ifnull_const_values==';
SELECT IFNULL(1, 1);
SELECT IFNULL(2, 1);
SELECT IFNULL(1, 2);
SELECT IFNULL(1, NULL);
SELECT IFNULL(NULL, 1);
SELECT IFNULL('a', 'a');
SELECT IFNULL('a', 'b');
SELECT IFNULL('b', 'a');
SELECT IFNULL('a', NULL);
SELECT IFNULL(NULL, 'a');
SELECT IFNULL(NULL, NULL);

SELECT '==ifnull_non_nullable_columns=';
CREATE TABLE IF NOT EXISTS t(a INT, b INT) ENGINE=Memory;
INSERT INTO t VALUES(0, 0), (0, 1), (1, 0), (1, 1);
SELECT a, b, IFNULL(a, b) FROM t;
DROP TABLE t;

SELECT '==ifnull_nullable_columns==';
CREATE TABLE IF NOT EXISTS t(a INT NULL, b INT NULL) ENGINE=Memory;
INSERT INTO t VALUES (0, NULL), (1, NULL), (NULL, 0), (NULL, 1), (NULL, NULL);
SELECT a, b, IFNULL(a, b) FROM t;
DROP TABLE t;

SET enable_planner_v2 = 0;

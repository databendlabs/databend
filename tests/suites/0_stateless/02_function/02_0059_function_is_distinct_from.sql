SET enable_planner_v2 = 1;

SELECT '==is_distinct_from_non_nullable_const_values==';
SELECT 1 IS DISTINCT FROM 2;
SELECT 1 IS DISTINCT FROM 1;
SELECT 'a' IS DISTINCT FROM 'b';
SELECT 'a' IS DISTINCT FROM 'a';


SELECT '==is_not_distinct_from_non_nullable_const_values==';
SELECT 1 IS NOT DISTINCT FROM 2;
SELECT 1 IS NOT DISTINCT FROM 1;
SELECT 'a' IS NOT DISTINCT FROM 'b';
SELECT 'a' IS NOT DISTINCT FROM 'a';

SELECT '==is_distinct_from_nullable_const_values==';
SELECT 1 IS DISTINCT FROM null;
SELECT null IS DISTINCT FROM 1;
SELECT 'a' IS DISTINCT FROM null;
SELECT null IS DISTINCT FROM 'b';
SELECT null IS DISTINCT FROM null;

SELECT '==is_not_distinct_from_nullable_const_values==';
SELECT 1 IS NOT DISTINCT FROM null;
SELECT null IS NOT DISTINCT FROM 1;
SELECT 'a' IS NOT DISTINCT FROM null;
SELECT null IS NOT DISTINCT FROM 'b';
SELECT null IS NOT DISTINCT FROM null;

SELECT '==is_distinct_non_nullable_columns==';
CREATE TABLE IF NOT EXISTS t(a INT, b INT) ENGINE=Memory;
INSERT INTO t VALUES(0, 0), (0, 1), (1, 0), (1, 1);
SELECT a, b, a is distinct FROM b FROM t;
DROP TABLE t;

SELECT '==is_not_distinct_non_nullable_columns==';
CREATE TABLE IF NOT EXISTS t(a INT, b INT) ENGINE=Memory;
INSERT INTO t VALUES(0, 0), (0, 1), (1, 0), (1, 1);
SELECT a, b, a IS NOT DISTINCT FROM b FROM t;
DROP TABLE t;

SELECT '==is_distinct_nullable_columns==';
CREATE TABLE IF NOT EXISTS t(a INT NULL, b INT NULL) ENGINE=Memory;
INSERT INTO t VALUES (0, NULL),  (NULL, 0), (NULL, NULL);
SELECT a, b , a IS DISTINCT FROM b FROM t;
DROP TABLE t;

SELECT '==is_distinct_nullable_columns==';
CREATE TABLE IF NOT EXISTS t(a INT NULL, b INT NULL) ENGINE=Memory;
INSERT INTO t VALUES (0, NULL), (NULL, 0), (NULL, NULL);
SELECT a, b, a IS NOT DISTINCT FROM b FROM t;
DROP TABLE t;

SET enable_planner_v2 = 0;

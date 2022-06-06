-- SET enable_planner_v2 = 1;

-- SELECT '==nullif_const_values==';
-- SELECT NULLIF(1, 1);
-- SELECT NULLIF(2, 1);
-- SELECT NULLIF(1, 2);
-- SELECT NULLIF(1, NULL);
-- SELECT NULLIF(NULL, 1);
-- SELECT NULLIF('a', 'a');
-- SELECT NULLIF('a', 'b');
-- SELECT NULLIF('b', 'a');
-- SELECT NULLIF('a', NULL);
-- SELECT NULLIF(NULL, 'a');
-- SELECT NULLIF(NULL, NULL);

-- SELECT '==nullif_non_nullable_columns=';
-- CREATE TABLE IF NOT EXISTS t(a INT, b INT) ENGINE=Memory;
-- INSERT INTO t VALUES(0, 0), (0, 1), (1, 0), (1, 1);
-- SELECT a, b, NULLIF(a, b) FROM t;
-- DROP TABLE t;

-- SELECT '==nullif_nullable_columns==';
-- CREATE TABLE IF NOT EXISTS t(a INT NULL, b INT NULL) ENGINE=Memory;
-- INSERT INTO t VALUES(0, 0), (0, 1), (0, NULL), (1, 0), (1, 1), (1, NULL), (NULL, 0), (NULL, 1), (NULL, NULL);
-- SELECT a, b, NULLIF(a, b) FROM t;
-- DROP TABLE t;

-- SELECT '==nullif_lhs_all_null==';
-- CREATE TABLE IF NOT EXISTS t(a INT NULL, b INT NULL) ENGINE=Memory;
-- INSERT INTO t VALUES(NULL, 0), (NULL, 1), (NULL, NULL), (NULL, 0), (NULL, 1);
-- SELECT a, b, NULLIF(a, b) FROM t;
-- DROP TABLE t;

-- SELECT '==nullif_rhs_all_null==';
-- CREATE TABLE IF NOT EXISTS t(a INT NULL, b INT NULL) ENGINE=Memory;
-- INSERT INTO t VALUES(0, NULL), (1, NULL), (0, NULL), (1, NULL), (NULL, NULL);
-- SELECT a, b, NULLIF(a, b) FROM t;
-- DROP TABLE t;

-- SET enable_planner_v2 = 0;
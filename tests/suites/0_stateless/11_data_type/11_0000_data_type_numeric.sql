-- For each data type:
-- 1) bound check
-- 2) conversion check
-- 3) group by check
CREATE DATABASE IF NOT EXISTS data_type;
USE data_type;

DROP TABLE IF EXISTS t;
CREATE TABLE t(tiny TINYINT, tiny_unsigned TINYINT UNSIGNED);

-- low bound check
INSERT INTO t VALUES (-1, -1);
SELECT * FROM t;

-- group by check
-- https://github.com/datafuselabs/databend/issues/4891
SELECT sum(tiny) FROM t GROUP BY tiny;

-- low bound check
TRUNCATE TABLE t;
INSERT INTO t VALUES (-129, -1);
SELECT * FROM t;

-- upper bound check
TRUNCATE TABLE t;
INSERT INTO t VALUES (128, 256);
SELECT * FROM t;

-- others(TODO)

DROP DATABASE data_type;

set enable_planner_v2 = 1;

-- No data when Engine is Null
DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0(a int) ENGINE = Null;
INSERT INTO TABLE t0 values(1);
SELECT * FROM t0;

RENAME TABLE t0 TO t1;
DROP TABLE t0; -- {ErrorCode 1025}
SELECT * FROM t1;

RENAME TABLE t1 to system.t1; -- {ErrorCode 1002}
DROP TABLE IF EXISTS t1;

-- No data after rename when Engine is Memory
DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0(a int) ENGINE = Memory;
INSERT INTO TABLE t0 values(1);
SELECT * FROM t0;

RENAME TABLE t0 TO t1;
DROP TABLE t0; -- {ErrorCode 1025}
SELECT * FROM t1;

RENAME TABLE t1 to system.t1; -- {ErrorCode 1002}
DROP TABLE IF EXISTS t1;

-- Data exists before and after rename
DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0(a int);
INSERT INTO TABLE t0 values(1);
SELECT * FROM t0;

RENAME TABLE t0 TO t1;
DROP TABLE t0; -- {ErrorCode 1025}
SELECT * FROM t1;

RENAME TABLE t1 to system.t1; -- {ErrorCode 1002}
DROP TABLE IF EXISTS t1;

set enable_planner_v2 = 0;

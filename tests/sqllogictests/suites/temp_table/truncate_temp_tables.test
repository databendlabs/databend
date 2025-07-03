statement ok
CREATE TEMP TABLE t1(c1 int)

statement ok
INSERT INTO TABLE t1 values(1)

statement ok
SELECT * FROM t1

statement ok
TRUNCATE TABLE t1

statement ok
DROP TABLE IF EXISTS t1

statement ok
DROP TABLE IF EXISTS t2

statement ok
CREATE TEMP TABLE t2(c1 int) ENGINE = FUSE

statement ok
INSERT INTO TABLE t2 values(1)

query I
SELECT * FROM t2
----
1

statement ok
TRUNCATE TABLE t2

statement ok
SELECT * FROM t2


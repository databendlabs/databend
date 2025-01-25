statement ok
CREATE TEMP TABLE t0(a int)

statement ok
INSERT INTO TABLE t0 values(1)

statement ok
SELECT * FROM t0

statement ok
RENAME TABLE t0 TO t1

statement error 1025
DROP TABLE t0

statement ok
SELECT * FROM t1

statement error 1006
RENAME TABLE t1 to system.t1

statement ok
DROP TABLE IF EXISTS t1

statement ok
DROP TABLE IF EXISTS t0

statement ok
DROP TABLE IF EXISTS t1

statement ok
CREATE TEMP TABLE t0(a int) ENGINE = Fuse

statement ok
INSERT INTO TABLE t0 values(1)

query I
SELECT * FROM t0
----
1

statement ok
RENAME TABLE t0 TO t1

statement error 1025
DROP TABLE t0

query I
SELECT * FROM t1
----
1

statement error 1006
RENAME TABLE t1 to system.t1

statement ok
DROP TABLE IF EXISTS t1

statement ok
DROP TABLE IF EXISTS t0

statement ok
DROP TABLE IF EXISTS t1

statement ok
CREATE TEMP TABLE t0(a int)

statement ok
INSERT INTO TABLE t0 values(1)

query I
SELECT * FROM t0
----
1

statement ok
RENAME TABLE t0 TO t1

statement error 1025
DROP TABLE t0

query I
SELECT * FROM t1
----
1

statement ok
CREATE TEMP TABLE t2(c int)

statement error 2302
RENAME TABLE t1 to t2

statement error 1006
RENAME TABLE t1 to system.t1


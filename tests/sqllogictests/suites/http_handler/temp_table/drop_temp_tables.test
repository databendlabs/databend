statement ok
CREATE TEMPORARY TABLE t(c1 int)

statement ok
DROP TABLE t

statement ok
DROP TABLE IF EXISTS t

statement error 1025
DROP TABLE t

statement ok
DROP TABLE if exists database_error.t

statement ok
DROP TABLE IF EXISTS catalog_error.database_error.t

statement error 1003
DROP TABLE database_error.t

statement error 1119
DROP TABLE catalog_error.database_error.t

statement error 1025
DROP TABLE system.abc

statement ok
DROP TABLE if exists system.abc

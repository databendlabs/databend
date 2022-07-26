DROP TABLE IF EXISTS t;

CREATE TABLE t(a bigint null, b int null, c varchar(255) null, d smallint, e Date ) ENGINE = Null;

DESCRIBE t;
DESC t;

CREATE TABLE t1(a bigint null, b int null, c char(255) null, d smallint, e Date, f char(120) not null default '' ) ENGINE = Null;

DESCRIBE t1;
DESC t1;

-- VIEW table
DESC INFORMATION_SCHEMA.COLUMNS;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t1;

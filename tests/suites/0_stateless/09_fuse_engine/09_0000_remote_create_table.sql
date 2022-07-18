DROP DATABASE IF EXISTS db1;

CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(a int, b varchar) Engine = fuse cluster by(a);
SELECT * FROM system.tables WHERE database='db1';

DROP TABLE t1;
DROP TABLE IF EXISTS t1;
DROP TABLE t1; -- {ErrorCode 1025}


-- create table with reserved table option should failed
CREATE TABLE t(c int) Engine = fuse database_id = 1; -- {ErrorCode 1022}
CREATE TABLE t(c int) Engine = fuse DATABASE_ID = 1; -- {ErrorCode 1022}

-- deprecated table option not allowed 
CREATE TABLE t(c int) Engine = fuse snapshot_loc = 1; -- {ErrorCode 1022}
CREATE TABLE t(c int) Engine = fuse SNAPSHOT_LOC = 1; -- {ErrorCode 1022}

DROP DATABASE db1;

DROP DATABASE IF EXISTS db1;

DROP DATABASE db1; -- {ErrorCode 1003}




DROP DATABASE IF EXISTS db1;

CREATE DATABASE db1;

USE db1;

select '-- create transient table';
CREATE TRANSIENT TABLE IF NOT EXISTS t09_0016(a int);

select '-- 3 insertions';
INSERT INTO t09_0016 VALUES(1);
INSERT INTO t09_0016 VALUES(2);
INSERT INTO t09_0016 VALUES(3);

select '-- check ingestion';
select * from t09_0016 order by a;

select '-- check history count, there should be only one snapshot left';
select count(*)=1 from fuse_snapshot('db1', 't09_0016');

DROP TABLE t09_0016;
DROP DATABASE db1;

DROP DATABASE IF EXISTS db_09_004;
CREATE DATABASE db_09_004;
USE db_09_004;

-- the same as 12_0000_insert_into_select.sql, expect there the engine is fuse
CREATE TABLE IF NOT EXISTS t1(a UInt8, b UInt64, c Int8, d Int64, e Date16, f Date32, g DateTime32, h String) Engine = Memory;
CREATE TABLE IF NOT EXISTS t3(a String, b String, c String, d String) Engine = Fuse;

INSERT INTO t1 (a,b,c,d,e) select * from t3; -- {ErrorCode 6}
INSERT INTO t1 (a,b,c,d,e) select a,b,c from t3; -- {ErrorCode 6}

-- extras
create table n1(a uint64);
insert into n1 select number from numbers(10000);
select sum(a) from n1;

CREATE TABLE n2(a UInt64, b UInt64);
insert into n2 select number, number + 1 from numbers(10000);
select count(*) from n2;
select sum(a), sum(b) from n2;

-- "self reference"
insert into n2 select * from n2;
select count(*) from n2;
select sum(a), sum(b) from n2;

-- aggregation
create table n3(a uint64, b uint64);
insert into n3 select sum(a), sum(b) from n2;
select * from n3;

-- cast
create table s1(a String, b String);
insert into s1 select number, number + 1 from numbers(10000);
select sum(cast(a as uint64)), sum(cast(b as uint64)) from s1;

DROP TABLE t1;
DROP TABLE t3;
DROP TABLE n1;
DROP TABLE n2;
DROP TABLE n3;
DROP TABLE s1;

DROP DATABASE db_09_004;

DROP DATABASE IF EXISTS db_09_004;
CREATE DATABASE db_09_004;
USE db_09_004;

-- the same as 12_0000_insert_into_select.sql, expect the engine here is fuse
CREATE TABLE IF NOT EXISTS t1(a UInt8, b UInt64, c Int8, d Int64, e Date16, f Date32, g DateTime32, h String) Engine = Memory;
CREATE TABLE IF NOT EXISTS t3(a String, b String, c String, d String) Engine = Fuse;

INSERT INTO t1 (a,b,c,d,e) select * from t3; -- {ErrorCode 1006}
INSERT INTO t1 (a,b,c,d,e) select a,b,c from t3; -- {ErrorCode 1006}

-- extras
create table n1(a uint64);
insert into n1 select number from numbers(10000);
select sum(a) from n1;

CREATE TABLE n2(a UInt64, b UInt64);
insert into n2 select number, number + 1 from numbers(10000);
select count(a) from n2;
select sum(a), sum(b) from n2;

-- "self reference"
insert into n2 select * from n2;
select count(a) from n2;
select sum(a), sum(b) from n2;

-- aggregation
create table n3(a uint64, b uint64);
insert into n3 select sum(a), sum(b) from n2;
select * from n3;

-- cast
create table s1(a String, b String);
insert into s1 select number, number + 1 from numbers(10000);
select sum(cast(a as uint64)), sum(cast(b as uint64)) from s1;


-- default
create table d1(n String, a UInt8 not null, b Int16 default a + 3, c String default 'c');
insert into d1(a) values (1);
insert into d1(b) values (2);
-- https://github.com/datafuselabs/databend/issues/3636
insert into d1(b) select b from d1;
select * from d1 order by a, b;



DROP TABLE t1;
DROP TABLE t3;
DROP TABLE n1;
DROP TABLE n2;
DROP TABLE n3;
DROP TABLE s1;
DROP TABLE d1;

DROP DATABASE db_09_004;

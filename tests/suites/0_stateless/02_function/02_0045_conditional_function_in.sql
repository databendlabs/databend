SELECT number from numbers_mt(10) WHERE number+1 IN (2, 3 ,20) ORDER BY number;
SELECT number from numbers_mt(10000) WHERE number+1 IN (2, 3 ,20, 5000) ORDER BY number;
SELECT number from numbers_mt(10) WHERE number+1 not IN (2, 3 ,20, 5000) ORDER BY number;
SELECT NULL IN (1, 2, 3);
SELECT 1 IN (1, 2, NULL);
SELECT 1 IN (2, 3, NULL);
SELECT 1.1 IN (1.10, 2, 3.3, 1.1);
select 1 in (1.2, 2);
select 1 in (1.0, 2);
select 'aa' in ('aa', 'bb', 'cc');
select true in (true, false);
select true in (NULL, false);
SELECT toDate(18869) in (toDate(18869));
SELECT toDate(18869) in ('2021-08-30');
SELECT toDate(18869) in (toDate(18869), NULL);
SELECT NULL in ('2021-08-30');
--
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(a UInt64 null, b UInt64 null) Engine = Memory;
INSERT INTO t1 (a,b) VALUES (1, NULL), (2, 3);
SELECT a FROM t1 WHERE b IN (NULL,3);
DROP TABLE t1;
--
DROP TABLE IF EXISTS t2;
CREATE TABLE t2(date DATE, datetime DATETIME null) Engine = Memory;
INSERT INTO t2 VALUES ('2022-04-23', '2022-04-23 01:48:19'), ('2022-04-24', NULL);
SELECT * FROM t2 WHERE date in ('2022-04-23');
SELECT * FROM t2 WHERE datetime in ('2022-04-23 01:48:19', NULL);
DROP TABLE t2;

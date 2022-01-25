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
-- 
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(a UInt64, b UInt64) Engine = Memory;
INSERT INTO t1 (a,b) VALUES (1, NULL), (2, 3);
SELECT a FROM t1 WHERE b IN (NULL,3);

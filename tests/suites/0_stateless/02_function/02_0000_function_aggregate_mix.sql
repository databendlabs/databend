SELECT sum(number) from numbers_mt(10000);
SELECT min(number) from numbers_mt(10000);
SELECT max(number) from numbers_mt(10000);
SELECT avg(number) from numbers_mt(10000);
SELECT count(number) from numbers_mt(10000);
SELECT sum(number)/count(number) from numbers_mt(10000);
SELECT arg_min(number, number) from numbers_mt(10000);
SELECT arg_min(a, b) from (select number + 5 as a, number - 5 as b from numbers_mt(10000));
SELECT arg_min(b, a) from (select number + 5 as a, number - 5 as b from numbers_mt(10000));
SELECT arg_max(number, number) from numbers_mt(10000);
SELECT arg_max(a, b) from (select number + 5 as a, number - 5 as b from numbers_mt(10000));
SELECT arg_max(b, a) from (select number + 5 as a, number - 5 as b from numbers_mt(10000));

-- test arg_max, arg_min fro String
SELECT arg_max(a, b) from (select number + 5 as a, cast(number as varchar(255)) as b from numbers_mt(10000)) ;
SELECT arg_max(b, a) from (select number + 5 as a, cast(number as varchar(255)) as b from numbers_mt(10000)) ;


select count(distinct number, number + 1 , number + 3 ) from ( select number % 100 as number from numbers(100000));
select count(distinct 3) from numbers(10000);
select uniq(number, number + 1 , number + 3 )  =  count(distinct number, number + 1 , number + 3 ) from ( select number % 100 as number from numbers(100000));
SELECT std(number) between  2886.751 and 2886.752 from numbers_mt(10000);
SELECT stddev(number) between  2886.751 and 2886.752 from numbers_mt(10000);
SELECT stddev_pop(number) between  2886.751 and 2886.752 from numbers_mt(10000);
SELECT covar_samp(number, number) from (select * from numbers_mt(5) order by number asc);
SELECT covar_pop(number, number) from (select * from numbers_mt(5) order by number asc);

DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

SELECT '==Variant==';
CREATE TABLE IF NOT EXISTS t1(id Int null, var Variant null) Engine = Fuse;

INSERT INTO t1 SELECT 1, parse_json('{"k":"v"}');
INSERT INTO t1 SELECT 2, parse_json('"abcd"');

SELECT max(var), min(var) FROM t1;
SELECT arg_max(id, var), arg_min(id, var) FROM (SELECT id, var FROM t1);

INSERT INTO t1 SELECT 3, parse_json('[1,2,3]');
INSERT INTO t1 SELECT 4, parse_json('10');

SELECT max(var), min(var) FROM t1;
SELECT arg_max(id, var), arg_min(id, var) FROM (SELECT id, var FROM t1);

INSERT INTO t1 SELECT 5, parse_json('null');
INSERT INTO t1 SELECT 6, parse_json('true');

SELECT max(var), min(var) FROM t1;
SELECT arg_max(id, var), arg_min(id, var) FROM (SELECT id, var FROM t1);

SELECT '==Array(Int32)==';

CREATE TABLE IF NOT EXISTS t2(id Int null, arr Array(Int32) null) Engine = Fuse;

INSERT INTO t2 VALUES(1, [1,2,3]);
INSERT INTO t2 VALUES(2, [1,2,4]);
INSERT INTO t2 VALUES(3, [3,4,5]);
SELECT max(arr), min(arr) FROM t2;
SELECT arg_max(id, arr), arg_min(id, arr) FROM (SELECT id, arr FROM t2);

DROP DATABASE db1;

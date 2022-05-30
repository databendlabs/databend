set enable_planner_v2 = 0;
SELECT max(number) FROM numbers_mt(0) GROUP BY number % 4;
SELECT max(number) FROM numbers_mt (10) WHERE number > 99999999998 GROUP BY number % 3;
SELECT avg(number), max(number+1)+1 FROM numbers_mt(10000) where number > 2;
SELECT number%3 as c1, number%2 as c2 FROM numbers_mt(10000) where number > 2 group by number%3, number%2 order by c1,c2;

SELECT number%3 as c1 FROM numbers_mt(10) where number > 2 group by number%3 order by c1;

-- https://github.com/datafuselabs/databend/issues/1786
SELECT count(*), name FROM system.credits WHERE name='ahash' GROUP BY name;
SELECT count(1), name FROM system.credits WHERE name='ahash' GROUP BY name;

-- SELECT 'NOT in GROUP BY function check';
-- SELECT number%3 as c1, number as c2 FROM numbers_mt(10) where number > 2 group by c1 order by c1;

SELECT '==GROUP BY Strings==';
SELECT a,b,count() from (SELECT cast((number%4) AS bigint) as a, cast((number%20) AS bigint) as b from numbers(100)) group by a,b order by a,b limit 3 ;


SELECT '==GROUP BY nullables==';

CREATE TABLE t(a UInt64 null, b UInt32 null, c UInt32) Engine = Fuse;
INSERT INTO t(a, b, c) VALUES (NULL, 2, 3), (2, 3, 4), (NULL, 4, 5), (3, 5, 6), (2, 6, 7);
-- nullable(u8)
SELECT a%3 as a1, sum(c) as c from t GROUP BY a1 ORDER BY c;

-- nullable(u8), nullable(u8)
SELECT a%2 as a1, a%3 as a2, count(0) as ct FROM t GROUP BY a1, a2, c ORDER BY c;

-- nullable(u8), u64
-- SELECT a%2 as a1, to_uint64(c % 3) as c, count(0) as ct FROM t GROUP BY a1, c ORDER BY a1, c, ct;
-- u64, nullable(u8)
-- SELECT to_uint64(c % 3) as c, a%2 as a1, count(0) as ct FROM t GROUP BY a1, c ORDER BY a1, c, ct;
DROP table t;

SELECT '==GROUP BY DATETIMES==';

CREATE TABLE t_datetime(created_at Date, created_time DateTime, count Int32);

insert into t_datetime select to_date('2022-04-01') + number % 2,  to_datetime('2022-04-01 00:00:00') + number % 2, 1 from numbers(10);
select created_at, sum(count) from t_datetime group by created_at order by created_at;
select created_time, sum(count) from t_datetime group by created_time order by created_time;

drop table t_datetime;

--
SELECT number, count(*) FROM numbers_mt(10000) group by number order by number limit 5;
set group_by_two_level_threshold=10;
SELECT number, count(*) FROM numbers_mt(1000) group by number order by number limit 5;
set group_by_two_level_threshold=1000000000;
SELECT number, count(*) FROM numbers_mt(1000) group by number order by number limit 5;

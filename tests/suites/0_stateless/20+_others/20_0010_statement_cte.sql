set enable_planner_v2 = 1;

-- test case from clickhouse

-- 1

select '====1====';

DROP TABLE IF EXISTS test1;

CREATE TABLE test1(i int, j int);

INSERT INTO test1 VALUES (1, 2), (3, 4);

WITH test1 AS (SELECT * FROM numbers(5)) SELECT * FROM test1;
WITH test1 AS (SELECT i + 1, j + 1 FROM test1) SELECT * FROM test1;
WITH test1 AS (SELECT i + 1, j + 1 FROM test1) SELECT * FROM (SELECT * FROM test1);

-- error
-- SELECT * FROM (WITH test1 AS (SELECT to_int32(number) i FROM numbers(5)) SELECT * FROM test1) l INNER JOIN test1 r on l.i = r.i;

-- not supported
-- WITH test1 AS (SELECT i + 1, j + 1 FROM test1) SELECT to_int64(4) i, to_int64(5) j FROM numbers(3) WHERE (i, j) IN test1;

DROP TABLE IF EXISTS test1;

WITH test1 AS (SELECT number-1 as n FROM numbers(42)) 
SELECT max(n+1)+1 z FROM test1;

-- not supported
-- WITH test1 AS (SELECT number-1 as n FROM numbers(42)) 
-- SELECT max(test1.n+1)+1 z FROM test1 join test1 x using (n) having z - 1 = (select min(n-1)+41 from test1) + 2;

WITH test1 AS (SELECT number-1 as n FROM numbers(4442) order by n limit 100)
SELECT max(n) FROM test1 where n=422;

WITH test1 AS (SELECT number-1 as n FROM numbers(4442) order by n limit 100)
SELECT max(n) FROM test1 where n=42;

drop table if exists with_test ;
create table with_test(n int64 null);
insert into with_test select number - 1 from numbers(10000);

-- not supported
-- WITH test1 AS (SELECT n FROM with_test where n <= 40) 
-- SELECT max(test1.n+1)+1 z FROM test1 join test1 x using (n) having max(test1.n+1)+1 - 1 = (select min(n-1)+41 from test1) + 2;

-- not supported
-- WITH test1 AS (SELECT n FROM with_test where n <= 40) 
-- SELECT max(test1.n+1)+1 z FROM test1 join test1 x using (n) having z - 1 = (select min(n-1)+41 from test1) + 2;

WITH test1 AS (SELECT  n FROM with_test order by n limit 100)
SELECT max(n) FROM test1 where n=422;

WITH test1 AS (SELECT n FROM with_test order by n limit 100)
SELECT max(n) FROM test1 where n=42;

WITH test1 AS (SELECT n FROM with_test where n = 42  order by n limit 100)
SELECT max(n) FROM test1 where n=42;

WITH test1 AS (SELECT n FROM with_test where n = 42 or 1=1 order by n limit 100)
SELECT max(n) FROM test1 where n=42;

-- not supported
-- WITH test1 AS (SELECT n, null as b FROM with_test where n = 42 or b is null order by n limit 100)
-- SELECT max(n) FROM test1 where n=42;

-- not supported
-- WITH test1 AS (SELECT n, null b FROM with_test where b is null)
-- SELECT max(n) FROM test1 where n=42;

-- not supported
-- WITH test1 AS (SELECT n, null b FROM with_test where b is null or 1=1)
-- SELECT max(n) FROM test1 where n=45;

-- not supported
-- WITH test1 AS (SELECT n, null b FROM with_test where b is null and n = 42)
-- SELECT max(n) FROM test1 where n=45;

WITH test1 AS (SELECT n, null b FROM with_test where 1=1 and n = 42 order by n)
SELECT max(n) FROM test1 where n=45;

WITH test1 AS (SELECT n, null b, n+1 m FROM with_test where 1=0 or n = 42 order by n limit 4)
SELECT max(n) m FROM test1 where test1.m=43 having max(n)=42;

-- not supported
-- WITH test1 AS (SELECT n, null b, n+1 m FROM with_test where  n = 42 order by n limit 4)
-- SELECT max(n) m FROM test1 where b is null and test1.m=43 having m=42 limit 4;

with
    test1 as (select n, null b, n+1 m from with_test where  n = 42 order by n limit 4),
    test2 as (select n + 1 as x, n - 1 as y from test1),
    test3 as (select x * y as z from test2)
select z + 1 as q from test3;

drop table  with_test ;

-- open after https://github.com/datafuselabs/databend/issues/6628, some results are error now
-- 2
select '====2====';

-- WITH 
-- x AS (SELECT number AS a FROM numbers(10)),
-- y AS (SELECT number AS a FROM numbers(5))
-- SELECT * FROM x WHERE a in (SELECT a FROM y)
-- ORDER BY a;

-- WITH 
-- x AS (SELECT number AS a FROM numbers(10)),
-- y AS (SELECT number AS a FROM numbers(5))
-- SELECT * FROM x left JOIN y USING (a)
-- ORDER BY a;

-- WITH 
-- x AS (SELECT number AS a FROM numbers(10)),
-- y AS (SELECT number AS a FROM numbers(5))
-- SELECT * FROM x JOIN y USING (a)
-- ORDER BY x.a;

-- WITH 
-- x AS (SELECT number AS a FROM numbers(10)),
-- y AS (SELECT number AS a FROM numbers(5)),
-- z AS (SELECT 1::UInt64 b)
-- SELECT * FROM x JOIN y USING (a) WHERE x.a in (SELECT * FROM z);

-- WITH 
-- x AS (SELECT number AS a FROM numbers(10)),
-- y AS (SELECT number AS a FROM numbers(5)),
-- z AS (SELECT * FROM x WHERE a % 2),
-- w AS (SELECT * FROM y WHERE a > 0)
-- SELECT * FROM x JOIN y USING (a) WHERE x.a in (SELECT * FROM z)
-- ORDER BY x.a;

-- not supported
-- WITH 
-- x AS (SELECT number AS a FROM numbers(10)),
-- y AS (SELECT number AS a FROM numbers(5)),
-- z AS (SELECT * FROM x WHERE a % 2),
-- w AS (SELECT * FROM y WHERE a > 0)
-- SELECT max(x.a) FROM x JOIN y USING (a) WHERE x.a in (SELECT * FROM z)
-- HAVING x.a > (SELECT min(a) FROM w);

-- not supported
-- WITH 
-- x AS (SELECT number AS a FROM numbers(10)),
-- y AS (SELECT number AS a FROM numbers(5)),
-- z AS (SELECT * FROM x WHERE a % 2),
-- w AS (SELECT * FROM y WHERE a > 0)
-- SELECT x.a FROM x JOIN y USING (a) WHERE x.a in (SELECT * FROM z)
-- HAVING x.a <= (SELECT max(a) FROM w)
-- ORDER BY x.a;


-- open after https://github.com/datafuselabs/databend/issues/6628, some results are error now
-- 3
select '====3====';

-- DROP TABLE IF EXISTS cte1;
-- DROP TABLE IF EXISTS cte2;

-- CREATE TABLE cte1(a Int64);
-- CREATE TABLE cte2(a Int64);

-- INSERT INTO cte1 SELECT * FROM numbers(10000);
-- INSERT INTO cte2 SELECT * FROM numbers(5000);

-- WITH
-- x AS (SELECT * FROM cte1),
-- y AS (SELECT * FROM cte2),
-- z AS (SELECT * FROM x WHERE a % 2 = 1),
-- w AS (SELECT * FROM y WHERE a > 333)
-- SELECT max(x.a) 
-- FROM x JOIN y USING (a) 
-- WHERE x.a in (SELECT * FROM z) AND x.a <= (SELECT max(a) FROM w);

-- WITH
-- x AS (SELECT * FROM cte1),
-- y AS (SELECT * FROM cte2),
-- z AS (SELECT * FROM x WHERE a % 3 = 1),
-- w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
-- SELECT count(x.a) 
-- FROM x left JOIN y USING (a) 
-- WHERE x.a in (SELECT * FROM z) AND x.a <= (SELECT max(a) FROM w);

-- WITH
-- x AS (SELECT * FROM cte1),
-- y AS (SELECT * FROM cte2),
-- z AS (SELECT * FROM x WHERE a % 3 = 1),
-- w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
-- SELECT count(x.a) 
-- FROM x left JOIN y USING (a) 
-- WHERE x.a in (SELECT * FROM z);

-- WITH
-- x AS (SELECT a-4000 a FROM cte1 WHERE cte1.a >700),
-- y AS (SELECT * FROM cte2),
-- z AS (SELECT * FROM x WHERE a % 3 = 1),
-- w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
-- SELECT count(*) 
-- FROM x left JOIN y USING (a) 
-- WHERE x.a in (SELECT * FROM z);

-- WITH
-- x AS (SELECT a-4000 a FROM cte1 WHERE cte1.a >700),
-- y AS (SELECT * FROM cte2),
-- z AS (SELECT * FROM x WHERE a % 3 = 1),
-- w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
-- SELECT max(a), min(a), count(*) 
-- FROM x
-- WHERE a in (SELECT * FROM z) AND a <100;

-- WITH
-- x AS (SELECT a-4000 a FROM cte1 WHERE cte1.a >700),
-- y AS (SELECT * FROM cte2),
-- z AS (SELECT * FROM x WHERE a % 3 = 1),
-- w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
-- SELECT max(a), min(a), count(*) FROM x
-- WHERE  a <100;

-- need support `x AS (SELECT a-4000 a FROM cte1 AS t WHERE cte1.a >700);`
-- WITH
-- x AS (SELECT a-4000 a FROM cte1 AS t WHERE cte1.a >700), 
-- y AS (SELECT * FROM cte2),
-- z AS (SELECT * FROM x WHERE a % 3 = 1),
-- w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
-- SELECT max(a), min(a), count(*) 
-- FROM y
-- WHERE a <100;

-- WITH
-- x AS (SELECT a-4000 a FROM cte1 t WHERE t.a >700),
-- y AS (SELECT x.a a FROM x left JOIN cte1 USING (a)),
-- z AS (SELECT * FROM x WHERE a % 3 = 1),
-- w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
-- SELECT max(a), min(a), count(*) 
-- FROM y
-- WHERE a <100;

-- DROP TABLE cte1;
-- DROP TABLE cte2;


-- 4
select '====4====';

with it as ( select * from numbers(1) ) select i.number from it as i;
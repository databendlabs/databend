SET max_threads=16;

-- https://github.com/datafuselabs/databend/issues/574
-- SELECT 'filter push down: push (number+1) to filter';
-- EXPLAIN SELECT (number+1) as a from numbers_mt(10000) where a > 2;

SELECT 'limit push down: push (limit 10) to projection';

SELECT 'group by push down: push alias to group by';
EXPLAIN select max(number+1) as c1, (number%3+1) as c2 from numbers_mt(10000) group by c2;

SELECT 'projection push down: push (name and value) to read datasource';

create table a (a int not null, b int not null, c int not null) Engine = Fuse;
EXPLAIN select a from a where b > 10;
drop table a;

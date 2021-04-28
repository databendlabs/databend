SET max_threads=16;
SELECT 'filter push down: push (number+1) to filter';
EXPLAIN SELECT (number+1) as a from numbers_mt(10000) where a > 2;
SELECT '';

SELECT 'limit push down: push (limit 10) to projection';
EXPLAIN select (number+1) as c1, number as c2 from numbers_mt(10000) where (c1+c2+1)=1 limit 10;
SELECT '';

SELECT 'group by push down: push alias to group by';
EXPLAIN select max(number+1) as c1, avg(number%3+1) as c2 from numbers_mt(10000) group by c2;
SELECT '';

SELECT 'projection push down: push (name and value) to readdatasource';
EXPLAIN select name from system.settings where value > 10;

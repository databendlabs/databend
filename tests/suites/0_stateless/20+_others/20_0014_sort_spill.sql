SELECT '==TEST GLOBAL SORT==';
set sort_spilling_bytes_threshold_per_proc = 8;
drop table if exists t;
CREATE TABLE t (a INT,  b INT,  c BOOLEAN NULL);
INSERT INTO t VALUES (1, 9, true), (2, 8, false), (3, 7, NULL);
truncate table system.metrics;

SELECT c FROM t ORDER BY c;

SELECT '===================';

-- Test if the spill is activated.
set sort_spilling_bytes_threshold_per_proc = 0;
select metric, labels, sum(value::float) from system.metrics where metric like '%spill%total' group by metric, labels order by metric desc;
set sort_spilling_bytes_threshold_per_proc = 8;

SELECT '===================';

SELECT c FROM t ORDER BY c;
SELECT c FROM t ORDER BY c DESC;
SELECT a, b FROM t ORDER BY b;

SELECT '===================';

drop table if exists abc;
CREATE TABLE abc (  a INT,  b INT,  c INT,  d VARCHAR);
INSERT INTO abc VALUES (1, 2, 3, 'one'), (4, 5, 6, 'Two');
SELECT d FROM abc ORDER BY lower(d);
SELECT a FROM abc ORDER BY a DESC;

SELECT '===================';

drop table if exists xy;
CREATE TABLE xy(x INT NULL, y INT NULL);
INSERT INTO xy VALUES (2, NULL), (NULL, 6), (2, 5), (4, 8);
SELECT x, y FROM xy ORDER BY y NULLS FIRST;
SELECT x, y FROM xy ORDER BY y NULLS LAST;
SELECT x, y FROM xy ORDER BY y DESC NULLS FIRST;

SELECT '===================';

set sort_spilling_bytes_threshold_per_proc = 0;
select metric, labels, sum(value::float) from system.metrics where metric like '%spill%total' group by metric, labels order by metric desc;
set sort_spilling_bytes_threshold_per_proc = 8;

SELECT '===================';

-- Test single thread
set max_threads = 1;
INSERT INTO xy VALUES (NULL, NULL);
SELECT x, y FROM xy ORDER BY x, y DESC NULLS FIRST;
SELECT x, y FROM xy ORDER BY x NULLS LAST, y DESC NULLS FIRST;
SELECT x, y FROM xy ORDER BY x NULLS FIRST, y DESC NULLS LAST;
SELECT x, y FROM xy ORDER BY x NULLS FIRST, y DESC;


set max_threads = 16;

-- Test spill in Top-N scenario.
SELECT '==TEST TOP-N SORT==';

truncate table system.metrics;

SELECT '===================';

SELECT c FROM t ORDER BY c limit 1;

SELECT '===================';

set sort_spilling_bytes_threshold_per_proc = 0;
select metric, labels, sum(value::float) from system.metrics where metric like '%spill%total' group by metric, labels order by metric desc;
set sort_spilling_bytes_threshold_per_proc = 60;

SELECT '===================';

SELECT x, y FROM xy ORDER BY x, y DESC NULLS FIRST LIMIT 3;
SELECT x, y FROM xy ORDER BY x NULLS LAST, y DESC NULLS FIRST LIMIT 3;
SELECT x, y FROM xy ORDER BY x NULLS FIRST, y DESC NULLS LAST LIMIT 3;

SELECT '===================';

set sort_spilling_bytes_threshold_per_proc = 0;
select metric, labels, sum(value::float) from system.metrics where metric like '%spill%total' group by metric, labels order by metric desc;
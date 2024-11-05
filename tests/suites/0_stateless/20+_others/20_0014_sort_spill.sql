SELECT '==TEST GLOBAL SORT==';
SET max_vacuum_temp_files_after_query=0;
set sort_spilling_bytes_threshold_per_proc = 8;
DROP TABLE if EXISTS t;
DROP TABLE IF EXISTS temp_files_count;

CREATE TABLE temp_files_count(count INT, number INT);
CREATE TABLE t (a INT,  b INT,  c BOOLEAN NULL);
INSERT INTO t VALUES (1, 9, true), (2, 8, false), (3, 7, NULL);

INSERT INTO temp_files_count SELECT COUNT() as count, 1 as number FROM system.temp_files;

SELECT c FROM t ORDER BY c;

INSERT INTO temp_files_count SELECT COUNT() as count, 2 as number FROM system.temp_files;

SELECT '===================';

-- Test if the spill is activated.
set sort_spilling_bytes_threshold_per_proc = 0;
SELECT any_if(count, number = 2) - any_if(count, number = 1) FROM temp_files_count;
set sort_spilling_bytes_threshold_per_proc = 8;

SELECT '===================';
INSERT INTO temp_files_count SELECT COUNT() as count, 3 as number FROM system.temp_files;

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

INSERT INTO temp_files_count SELECT COUNT() as count, 4 as number FROM system.temp_files;
SELECT '===================';

set sort_spilling_bytes_threshold_per_proc = 0;
SELECT any_if(count, number = 4) - any_if(count, number = 3) FROM temp_files_count;
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

INSERT INTO temp_files_count SELECT COUNT() as count, 5 as number FROM system.temp_files;

SELECT '===================';

SELECT c FROM t ORDER BY c limit 1;

SELECT '===================';

INSERT INTO temp_files_count SELECT COUNT() as count, 6 as number FROM system.temp_files;

set sort_spilling_bytes_threshold_per_proc = 0;
SELECT any_if(count, number = 6) - any_if(count, number = 5) FROM temp_files_count;
set sort_spilling_bytes_threshold_per_proc = 60;

SELECT '===================';

INSERT INTO temp_files_count SELECT COUNT() as count, 7 as number FROM system.temp_files;

SELECT x, y FROM xy ORDER BY x, y DESC NULLS FIRST LIMIT 3;
SELECT x, y FROM xy ORDER BY x NULLS LAST, y DESC NULLS FIRST LIMIT 3;
SELECT x, y FROM xy ORDER BY x NULLS FIRST, y DESC NULLS LAST LIMIT 3;

SELECT '===================';
INSERT INTO temp_files_count SELECT COUNT() as count, 8 as number FROM system.temp_files;

unset max_vacuum_temp_files_after_query;
set sort_spilling_bytes_threshold_per_proc = 0;
SELECT any_if(count, number = 8) - any_if(count, number = 7) FROM temp_files_count;
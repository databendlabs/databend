#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

$BENDSQL_CLIENT_CONNECT --query="""
SELECT '==TEST GLOBAL SORT==';
SET max_vacuum_temp_files_after_query= 0;
set sort_spilling_bytes_threshold_per_proc = 8;
set enable_experimental_stream_sort_spilling = 0;
DROP TABLE if EXISTS t;
DROP TABLE IF EXISTS temp_files_count;

CREATE TABLE temp_files_count(count INT, number INT);
CREATE TABLE t (a INT,  b INT,  c BOOLEAN NULL);
INSERT INTO t VALUES (1, 9, true), (2, 8, false), (3, 7, NULL);

INSERT INTO temp_files_count SELECT COUNT() as count, 1 as number FROM system.temp_files;

SELECT c FROM t ORDER BY c;

INSERT INTO temp_files_count SELECT COUNT() as count, 2 as number FROM system.temp_files;

SELECT '==Test if the spill is activated==';

-- Test if the spill is activated.
set sort_spilling_bytes_threshold_per_proc = 0;
SELECT (any_if(count, number = 2) - any_if(count, number = 1)) > 0 FROM temp_files_count;
set sort_spilling_bytes_threshold_per_proc = 8;

SELECT '==Enable sort_spilling_bytes_threshold_per_proc==';
INSERT INTO temp_files_count SELECT COUNT() as count, 3 as number FROM system.temp_files;

SELECT c FROM t ORDER BY c;
SELECT c FROM t ORDER BY c DESC;
SELECT a, b FROM t ORDER BY b;

SELECT '==Test abc==';

drop table if exists abc;
CREATE TABLE abc (  a INT,  b INT,  c INT,  d VARCHAR);
INSERT INTO abc VALUES (1, 2, 3, 'one'), (4, 5, 6, 'Two');
SELECT d FROM abc ORDER BY lower(d);
SELECT a FROM abc ORDER BY a DESC;

SELECT '==Test xy==';

drop table if exists xy;
CREATE TABLE xy(x INT NULL, y INT NULL);
INSERT INTO xy VALUES (2, NULL), (NULL, 6), (2, 5), (4, 8);
SELECT x, y FROM xy ORDER BY y NULLS FIRST;
SELECT x, y FROM xy ORDER BY y NULLS LAST;
SELECT x, y FROM xy ORDER BY y DESC NULLS FIRST;

INSERT INTO temp_files_count SELECT COUNT() as count, 4 as number FROM system.temp_files;
SELECT '==Test a==';

set sort_spilling_bytes_threshold_per_proc = 0;
SELECT (any_if(count, number = 4) - any_if(count, number = 3)) > 0 FROM temp_files_count;
set sort_spilling_bytes_threshold_per_proc = 8;

SELECT '==Test b==';

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

SELECT '==Test c==';

SELECT c FROM t ORDER BY c limit 1;

SELECT '==Test d==';

INSERT INTO temp_files_count SELECT COUNT() as count, 6 as number FROM system.temp_files;

set sort_spilling_bytes_threshold_per_proc = 0;
SELECT (any_if(count, number = 6) - any_if(count, number = 5)) > 0 FROM temp_files_count;
set sort_spilling_bytes_threshold_per_proc = 60;

SELECT '==Test e==';

INSERT INTO temp_files_count SELECT COUNT() as count, 7 as number FROM system.temp_files;

SELECT x, y FROM xy ORDER BY x, y DESC NULLS FIRST LIMIT 3;
SELECT x, y FROM xy ORDER BY x NULLS LAST, y DESC NULLS FIRST LIMIT 3;
SELECT x, y FROM xy ORDER BY x NULLS FIRST, y DESC NULLS LAST LIMIT 3;

SELECT '==Test f==';

INSERT INTO temp_files_count SELECT COUNT() as count, 8 as number FROM system.temp_files;

unset max_vacuum_temp_files_after_query;
unset enable_experimental_stream_sort_spilling;
set sort_spilling_bytes_threshold_per_proc = 0;
SELECT (any_if(count, number = 8) - any_if(count, number = 7)) > 0 FROM temp_files_count;

SET max_vacuum_temp_files_after_query= 300000;
"""

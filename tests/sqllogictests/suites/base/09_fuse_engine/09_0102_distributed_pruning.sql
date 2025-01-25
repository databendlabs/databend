
statement ok
create or replace database distributed_pruning;

statement ok
use distributed_pruning;

statement ok
CREATE or replace TABLE test_table(TEXT String);


statement ok
INSERT INTO test_table VALUES('test_text1');

statement ok
INSERT INTO test_table VALUES('test_text2');

statement ok
INSERT INTO test_table VALUES('test_text3');

statement ok
INSERT INTO test_table VALUES('test_text4');

statement ok
SET enable_distributed_pruning = 1;

query TTTTT
SELECT COUNT() FROM test_table;
----
4

statement ok
SET enable_distributed_pruning = 0;

query TTTTT
SELECT COUNT() FROM test_table;
----
4

statement ok
SET enable_distributed_pruning = 1;

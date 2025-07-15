statement ok
set enable_compact_after_multi_table_insert = 1;

statement ok
create or replace database multi_table_insert_auto_compact;

statement ok
use multi_table_insert_auto_compact;

statement ok
set auto_compaction_imperfect_blocks_threshold = 3;

statement ok
create or replace table t1 (c int) block_per_segment = 10 row_per_block = 3;

statement ok
create or replace table t2 (c int) block_per_segment = 10 row_per_block = 3;

statement ok
create or replace table t3 (c int) block_per_segment = 10 row_per_block = 3;

# first block (after compaction)
# There is less one rows in t3
statement ok
insert all into t1 into t2 select 1;

statement ok
insert all into t1 into t2 into t3 select 1;

statement ok
insert all into t1 into t2 into t3 select 1;


# second block (after compaction)
statement ok
insert all into t1 into t2 into t3 select 1;

statement ok
insert all into t1 into t2 into t3 select 1;

statement ok
insert all into t1 into t2 into t3 select 1;


# third block (after compaction)
statement ok
insert all into t1 into t2 into t3 select 1;

statement ok
insert all into t1 into t2 into t3 select 1;

statement ok
insert all into t1 into t2 into t3 select 1;

query III
select segment_count , block_count , row_count from fuse_snapshot('multi_table_insert_auto_compact', 't1') limit 20;
----
1 3 9
4 5 9
3 4 8
2 3 7
1 2 6
4 4 6
3 3 5
2 2 4
1 1 3
3 3 3
2 2 2
1 1 1

query III
select segment_count , block_count , row_count from fuse_snapshot('multi_table_insert_auto_compact', 't2') limit 20;
----
1 3 9
4 5 9
3 4 8
2 3 7
1 2 6
4 4 6
3 3 5
2 2 4
1 1 3
3 3 3
2 2 2
1 1 1

query III
select segment_count , block_count , row_count from fuse_snapshot('multi_table_insert_auto_compact', 't3') limit 20;
----
3 4 8
2 3 7
1 2 6
4 4 6
3 3 5
2 2 4
1 1 3
3 3 3
2 2 2
1 1 1

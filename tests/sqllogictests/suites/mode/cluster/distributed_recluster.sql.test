statement ok
drop table if exists t_recluster

statement ok
create table t_recluster (a int not null) cluster by(a) row_per_block=3

statement ok
set recluster_block_size = 30

statement ok
insert into t_recluster select 10-number from numbers(20)

statement ok
insert into t_recluster select 10-number from numbers(20)

statement ok
insert into t_recluster select 10-number from numbers(20)

statement ok
set enable_distributed_recluster = 1

statement ok
alter table t_recluster recluster

# query I
# select count()>1 from system.metrics where metric='fuse_recluster_block_nums_to_read_total'
# ----
# 1

statement ok
set enable_distributed_recluster = 0

statement ok
drop table if exists t_recluster

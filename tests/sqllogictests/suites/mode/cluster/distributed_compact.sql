statement ok
drop table if exists t_compact

statement ok
create table t_compact (a int not null) row_per_block=5 block_per_segment=5

statement ok
insert into t_compact select 50 - number from numbers(100)

statement ok
insert into t_compact select 50 - number from numbers(100)

query II
select count(),sum(a) from t_compact
----
200 100

statement ok
alter table t_compact set options(row_per_block=10,block_per_segment=10)

# lazy compact
statement ok
optimize table t_compact compact

query I
select count() from fuse_snapshot('default', 't_compact')
----
3

statement ok
alter table t_compact cluster by(abs(a))

# nolazy compact
statement ok
optimize table t_compact compact

query I
select count() from fuse_snapshot('default', 't_compact')
----
6

query II
select count(),sum(a) from t_compact
----
200 100

query I
select row_count from fuse_snapshot('default', 't_compact') limit 1
----
200

statement ok
drop table if exists t_compact

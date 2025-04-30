statement ok
create or replace transient table t (c int) cluster by (c);

statement ok
insert into t values(1), (3), (5);

statement ok
insert into t values(2), (7), (9);

statement ok
alter table t recluster final;

statement ok
insert into t values(1), (3), (5);

statement ok
insert into t values(2), (7), (9);

statement ok
alter table t recluster final;
